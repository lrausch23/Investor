from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from src.app.auth import require_actor
from src.app.db import db_session
from src.core.tax_documents import (
    DOC_TYPES,
    aggregate_tax_doc_overrides,
    build_tax_reconciliation,
    build_facts_from_extraction,
    extract_tax_document,
    list_household_entities,
    latest_extraction,
    save_tax_document,
    serialize_tax_document,
    suggest_owner_entity_id,
    _set_custom_doc_type_notes,
)
from src.core.taxes import get_or_create_tax_inputs, normalize_tax_inputs
from src.db.models import TaxDocument, TaxDocumentExtraction, TaxFact, TaxReconciliationSnapshot


router = APIRouter(prefix="/api/taxes", tags=["tax-documents"])

_DOC_TYPE_ALIASES = {
    "W2": "W2",
    "W-2": "W2",
    "K1": "K1",
    "K-1": "K1",
    "1099INT": "1099INT",
    "1099-INT": "1099INT",
    "1099DIV": "1099DIV",
    "1099-DIV": "1099DIV",
    "1099B": "1099B",
    "1099-B": "1099B",
    "1099R": "1099R",
    "1099-R": "1099R",
    "1095A": "1095A",
    "1095-A": "1095A",
    "1098": "1098",
    "SSA1099": "SSA1099",
    "SSA-1099": "SSA1099",
}


def _normalize_custom_doc_type(value: str) -> str | None:
    if not value:
        return None
    v = value.strip().upper().replace("—", "-").replace("–", "-")
    v = v.replace(" ", "")
    return _DOC_TYPE_ALIASES.get(v) or _DOC_TYPE_ALIASES.get(v.replace("-", "")) or None


@router.get("/entities")
def list_tax_entities(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tax_year: int = 0,
):
    year = int(tax_year or 0)
    entities = list_household_entities(session, tax_year=year)
    return JSONResponse({"ok": True, "entities": entities})


@router.get("/reconcile")
def reconcile_tax_documents(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tax_year: int = 0,
):
    year = int(tax_year or 0)
    data = build_tax_reconciliation(session, tax_year=year)
    return JSONResponse({"ok": True, "reconciliation": data})


@router.post("/documents/upload")
async def upload_tax_documents(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tax_year: int = Form(...),
    doc_type: str | None = Form(default=None),
    files: list[UploadFile] = File(...),
):
    entity_rows = list_household_entities(session, tax_year=int(tax_year))
    entities = {int(e["id"]): e for e in entity_rows}
    docs = []
    for upload in files:
        raw = await upload.read()
        if not raw:
            continue
        doc = save_tax_document(session, tax_year=int(tax_year), file_name=upload.filename or "upload.pdf", file_bytes=raw, actor=actor, doc_type=doc_type)
        extraction = latest_extraction(session, doc_id=doc.id)
        payload = serialize_tax_document(doc, extraction)
        suggested_owner = suggest_owner_entity_id(
            session,
            tax_year=int(tax_year),
            doc_type=doc.doc_type,
            extracted=extraction.extracted_json if extraction else {},
        )
        owner = entities.get(int(doc.owner_entity_id or 0))
        payload["suggested_owner_entity_id"] = suggested_owner
        payload["owner_label"] = owner.get("display_name") if owner else None
        payload["owner_type"] = owner.get("entity_type") if owner else None
        docs.append(payload)
    return JSONResponse({"ok": True, "documents": docs})


@router.get("/documents")
def list_tax_documents(
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tax_year: int = 0,
):
    year = int(tax_year or 0)
    entity_rows = list_household_entities(session, tax_year=year)
    entities = {int(e["id"]): e for e in entity_rows}
    docs = session.query(TaxDocument).filter(TaxDocument.tax_year == year).order_by(TaxDocument.uploaded_at.desc()).all()
    out = []
    for doc in docs:
        extraction = latest_extraction(session, doc_id=doc.id)
        payload = serialize_tax_document(doc, extraction)
        suggested_owner = suggest_owner_entity_id(
            session,
            tax_year=year,
            doc_type=doc.doc_type,
            extracted=extraction.extracted_json if extraction else {},
        )
        owner = entities.get(int(doc.owner_entity_id or 0))
        payload["suggested_owner_entity_id"] = suggested_owner
        payload["owner_label"] = owner.get("display_name") if owner else None
        payload["owner_type"] = owner.get("entity_type") if owner else None
        out.append(payload)
    return JSONResponse({"ok": True, "documents": out})


@router.post("/documents/{doc_id}/extract")
def extract_tax_document_api(
    doc_id: int,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    doc_type_override: str | None = Form(default=None),
    force_ocr: str = Form(default=""),
):
    doc = session.query(TaxDocument).filter(TaxDocument.id == int(doc_id)).one_or_none()
    if doc is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})
    if not doc.raw_file_path:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Document file missing"})
    doc.status = "EXTRACTING"
    if doc_type_override in DOC_TYPES:
        doc.doc_type = doc_type_override
    session.commit()

    try:
        extraction_json = extract_tax_document(Path(doc.raw_file_path), doc_type_hint=doc.doc_type, force_ocr=force_ocr == "on")
    except Exception as exc:
        doc.status = "ERROR"
        doc.notes = f"Extraction failed: {exc}"
        session.commit()
        return JSONResponse(status_code=500, content={"ok": False, "error": f"Extraction failed: {exc}"})
    warnings = extraction_json.get("warnings") or []
    confidence_overall = float(extraction_json.get("confidence_overall") or 0.0)
    extraction = TaxDocumentExtraction(
        tax_document_id=doc.id,
        extracted_json=extraction_json,
        confidence_overall=confidence_overall,
        warnings=warnings,
        extracted_at=dt.datetime.utcnow(),
        extractor_version="taxdoc_v1",
    )
    session.add(extraction)
    doc.status = "NEEDS_REVIEW" if warnings or confidence_overall < 0.5 else "EXTRACTED"
    session.commit()
    payload = serialize_tax_document(doc, extraction)
    suggested_owner = suggest_owner_entity_id(
        session,
        tax_year=int(doc.tax_year),
        doc_type=doc.doc_type,
        extracted=extraction_json,
    )
    payload["suggested_owner_entity_id"] = suggested_owner
    if doc.owner_entity_id:
        entity_rows = list_household_entities(session, tax_year=int(doc.tax_year))
        owner = next((row for row in entity_rows if int(row["id"]) == int(doc.owner_entity_id)), None)
        payload["owner_label"] = owner.get("display_name") if owner else None
        payload["owner_type"] = owner.get("entity_type") if owner else None
    return JSONResponse({"ok": True, "document": payload})


@router.patch("/documents/{doc_id}")
async def update_tax_document(
    doc_id: int,
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
):
    payload = await request.json()
    doc = session.query(TaxDocument).filter(TaxDocument.id == int(doc_id)).one_or_none()
    if doc is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "Document not found"})
    if "owner_entity_id" in payload:
        owner_raw = payload.get("owner_entity_id")
        doc.owner_entity_id = int(owner_raw) if str(owner_raw).isdigit() else None
    if "is_authoritative" in payload:
        doc.is_authoritative = bool(payload.get("is_authoritative"))
    if "is_corrected" in payload:
        doc.is_corrected = bool(payload.get("is_corrected"))
    if "notes" in payload:
        doc.notes = str(payload.get("notes") or "")
    session.commit()
    extraction = latest_extraction(session, doc_id=doc.id)
    doc_payload = serialize_tax_document(doc, extraction)
    return JSONResponse({"ok": True, "document": doc_payload})

@router.post("/documents/apply")
async def apply_tax_documents(
    request: Request,
    session: Session = Depends(db_session),
    actor: str = Depends(require_actor),
    tax_year: int = 0,
):
    payload = await request.json()
    year = int(payload.get("tax_year") or tax_year or 0)
    docs_payload = payload.get("documents") or []

    applied_docs = []
    for doc_payload in docs_payload:
        doc_id = int(doc_payload.get("id") or 0)
        confirmed = bool(doc_payload.get("confirmed"))
        doc = session.query(TaxDocument).filter(TaxDocument.id == doc_id).one_or_none()
        if doc is None:
            continue
        if doc_payload.get("doc_type") in DOC_TYPES:
            doc.doc_type = doc_payload.get("doc_type")
        doc.is_corrected = bool(doc_payload.get("is_corrected")) if "is_corrected" in doc_payload else doc.is_corrected
        if "is_authoritative" in doc_payload:
            flag = bool(doc_payload.get("is_authoritative"))
            if doc.is_corrected:
                doc.is_authoritative = flag
            else:
                doc.is_authoritative = True if flag else None
        custom_doc_type = str(doc_payload.get("custom_doc_type") or "").strip()
        normalized_custom = _normalize_custom_doc_type(custom_doc_type) if custom_doc_type else None
        if normalized_custom:
            doc.doc_type = normalized_custom
            doc.notes = _set_custom_doc_type_notes(doc.notes, None)
        elif custom_doc_type:
            doc.doc_type = "OTHER"
            doc.notes = _set_custom_doc_type_notes(doc.notes, custom_doc_type)
        elif doc_payload.get("doc_type") and doc_payload.get("doc_type") != "OTHER":
            doc.notes = _set_custom_doc_type_notes(doc.notes, None)

        owner_raw = doc_payload.get("owner_entity_id")
        if owner_raw not in (None, ""):
            doc.owner_entity_id = int(owner_raw) if str(owner_raw).isdigit() else None

        extraction = latest_extraction(session, doc_id=doc.id)
        if extraction is None:
            extraction = TaxDocumentExtraction(
                tax_document_id=doc.id,
                extracted_json={"doc_type": doc.doc_type, "fields": []},
                confidence_overall=0.0,
                warnings=["Manual entry (no extraction)"],
                extracted_at=dt.datetime.utcnow(),
                extractor_version="manual",
            )
            session.add(extraction)
            session.flush()
        extracted_json = dict(extraction.extracted_json or {})
        desired_doc_type = doc.doc_type or extracted_json.get("doc_type")
        should_reextract = False
        if desired_doc_type and extracted_json.get("doc_type") and extracted_json.get("doc_type") != desired_doc_type:
            should_reextract = True
        if desired_doc_type == "1095A":
            fields = extracted_json.get("fields") or []
            has_aca = any(str(f.get("key") or "").startswith("aca_") for f in fields)
            if not has_aca:
                should_reextract = True
        if should_reextract and doc.raw_file_path:
            try:
                fresh = extract_tax_document(Path(doc.raw_file_path), doc_type_hint=desired_doc_type)
                extracted_json = fresh
                extraction.extracted_json = extracted_json
                extraction.confidence_overall = fresh.get("confidence_overall")
                extraction.warnings = fresh.get("warnings") or []
            except Exception:
                # Fall back to previous extraction if re-parse fails.
                extracted_json = dict(extraction.extracted_json or {})
        field_overrides = doc_payload.get("fields") or {}
        if extracted_json.get("fields") and field_overrides:
            for field in extracted_json["fields"]:
                key = field.get("key")
                if key in field_overrides:
                    field["value_confirmed"] = field_overrides[key]
        manual_fields = doc_payload.get("manual_fields") or {}
        if manual_fields:
            fields = extracted_json.get("fields") or []
            field_by_key = {str(f.get("key")): f for f in fields if f.get("key")}
            for key, value in manual_fields.items():
                if value in (None, ""):
                    continue
                key_str = str(key)
                if key_str in field_by_key:
                    field_by_key[key_str]["value"] = str(value)
                    field_by_key[key_str]["value_confirmed"] = str(value)
                    field_by_key[key_str]["confidence"] = 1.0
                else:
                    fields.append(
                        {
                            "key": key_str,
                            "label": key_str.replace("_", " ").title(),
                            "value": str(value),
                            "value_confirmed": str(value),
                            "confidence": 1.0,
                        }
                    )
            extracted_json["fields"] = fields
        if doc.doc_type:
            extracted_json["doc_type"] = doc.doc_type
        extraction.extracted_json = extracted_json
        extraction.extracted_at = dt.datetime.utcnow()
        session.add(extraction)

        session.query(TaxFact).filter(TaxFact.source_doc_id == doc.id).delete()
        if confirmed:
            if doc.owner_entity_id is None:
                doc.owner_entity_id = suggest_owner_entity_id(
                    session,
                    tax_year=int(doc.tax_year),
                    doc_type=doc.doc_type,
                    extracted=extracted_json,
                )
            owner_overrides = {}
            for key, val in (doc_payload.get("fact_owners") or {}).items():
                if str(val).isdigit():
                    owner_overrides[str(key)] = int(val)
            facts = build_facts_from_extraction(doc, extracted_json, owner_overrides=owner_overrides)
            for fact in facts:
                session.add(TaxFact(**fact))
            doc.status = "CONFIRMED"
            applied_docs.append(doc.id)
        else:
            doc.status = "NEEDS_REVIEW"
        session.add(doc)

    session.commit()

    overrides = aggregate_tax_doc_overrides(session, tax_year=year)
    inputs_row = get_or_create_tax_inputs(session, year=year)
    data_json = normalize_tax_inputs(dict(inputs_row.data_json or {}))
    data_json["tax_doc_overrides"] = overrides
    data_json["docs_primary"] = True
    inputs_row.data_json = data_json
    inputs_row.updated_at = dt.datetime.utcnow()
    reconcile = build_tax_reconciliation(session, tax_year=year)
    session.add(
        TaxReconciliationSnapshot(
            tax_year=year,
            summary_json=reconcile,
            document_ids=reconcile.get("doc_ids") or [],
        )
    )
    session.commit()

    return JSONResponse({"ok": True, "applied_docs": applied_docs, "overrides": overrides})
