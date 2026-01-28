from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from src.db.models import HouseholdEntity, TaxDocument, TaxDocumentExtraction, TaxFact

EXTRACTOR_VERSION = "taxdoc_v1"

DATA_DIR = Path("data") / "tax_docs"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DOC_TYPES = ["W2", "K1", "1099INT", "1099DIV", "1099B", "1099R", "1095A", "1098", "SSA1099", "OTHER"]

FACT_TYPES = {
    "WAGES",
    "FED_WITHHOLDING",
    "IRA_DISTRIBUTION",
    "IRA_WITHHOLDING",
    "INT_INCOME",
    "DIV_ORDINARY",
    "DIV_QUALIFIED",
    "CAP_GAIN_DIST",
    "K1_ORD_INCOME",
    "K1_INTEREST",
    "K1_DIVIDENDS",
    "K1_RENTAL",
    "K1_OTHER",
    "ACA_PREMIUM",
    "ACA_SLCSP",
    "ACA_APTC",
    "B_PROCEEDS",
    "B_COST_BASIS",
    "B_GAIN_LOSS",
}

MIN_TEXT_CHARS = 20

DEFAULT_HOUSEHOLD_ENTITIES = [
    ("USER", "Laszlo"),
    ("SPOUSE", "Tamila"),
    ("DEPENDENT", "Milana"),
    ("TRUST", "Trust"),
    ("TRUST", "Kolozsi LLC"),
    ("TRUST", "Kolozsi Marine LLC"),
    ("BUSINESS", "ArtYoga LLC"),
    ("HOUSEHOLD", "Household"),
]

_AMOUNT_RE = re.compile(r"[-(]?\$?\d+(?:,\d{3})*(?:\.\d{2})?\)?")
_EIN_RE = re.compile(r"\b\d{2}-\d{7}\b")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_MONTHS = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
]


def _custom_doc_type_from_notes(notes: str | None) -> str | None:
    if not notes:
        return None
    for line in str(notes).splitlines():
        if line.strip().lower().startswith("custom_doc_type:"):
            return line.split(":", 1)[1].strip() or None
    return None


def _set_custom_doc_type_notes(notes: str | None, custom_type: str | None) -> str | None:
    base_lines = []
    if notes:
        for line in str(notes).splitlines():
            if not line.strip().lower().startswith("custom_doc_type:"):
                base_lines.append(line)
    if custom_type:
        base_lines.append(f"custom_doc_type: {custom_type.strip()}")
    return "\n".join([line for line in base_lines if line]).strip() or None


def _safe_filename(name: str) -> str:
    safe = Path(name or "upload.pdf").name
    return safe.replace("..", ".")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_amount(token: str | None) -> float | None:
    if not token:
        return None
    s = str(token).strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    if s.startswith("-"):
        neg = True
        s = s[1:]
    s = s.replace("$", "").replace(",", "").strip()
    try:
        val = float(s)
    except Exception:
        return None
    return -val if neg else val


def _extract_amounts(text: str) -> list[float]:
    out: list[float] = []
    for tok in _AMOUNT_RE.findall(text or ""):
        val = _parse_amount(tok)
        if val is not None:
            out.append(val)
    return out


def _find_amount(lines: list[str], keywords: list[str]) -> float | None:
    keys = [k.upper() for k in keywords]
    for line in lines:
        if all(k in line for k in keys):
            amounts = _extract_amounts(line)
            if amounts:
                return amounts[-1]
    return None


def _find_amount_with_fallback(
    lines: list[str],
    keywords: list[str],
    *,
    min_value: float = 10.0,
    lookahead: int = 3,
) -> float | None:
    keys = [k.upper() for k in keywords]
    for idx, line in enumerate(lines):
        if not all(k in line for k in keys):
            continue
        amounts = _extract_amounts(line)
        if amounts:
            max_amt = max(amounts, key=lambda v: abs(v))
            if abs(max_amt) > min_value:
                return max_amt
            # Try nearby lines for a larger amount (often the value is on the next line).
            for j in range(idx + 1, min(idx + 1 + lookahead, len(lines))):
                next_amounts = _extract_amounts(lines[j])
                if next_amounts:
                    next_max = max(next_amounts, key=lambda v: abs(v))
                    if abs(next_max) > abs(max_amt):
                        return next_max
            return max_amt
        # No amount on the keyword line; try following lines.
        for j in range(idx + 1, min(idx + 1 + lookahead, len(lines))):
            next_amounts = _extract_amounts(lines[j])
            if next_amounts:
                return max(next_amounts, key=lambda v: abs(v))
    return None


def _find_name_after(lines: list[str], keywords: list[str]) -> str | None:
    keys = [k.upper() for k in keywords]
    for idx, line in enumerate(lines):
        if all(k in line for k in keys):
            inline = line
            for k in keys:
                inline = inline.replace(k, "")
            inline = inline.replace(":", "").strip()
            if inline and not _AMOUNT_RE.search(inline):
                return inline
            for j in range(idx + 1, min(idx + 4, len(lines))):
                cand = lines[j].strip()
                if cand and not _AMOUNT_RE.search(cand):
                    return cand
    return None


def _mask_identifier(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) <= 4:
        return digits
    return f"***-**-{digits[-4:]}"


def _normalize_lines(text: str) -> list[str]:
    lines = []
    for raw in (text or "").splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line.upper())
    return lines


def _normalize_name(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^A-Z0-9]+", " ", str(value).upper())
    return " ".join(part for part in cleaned.split() if part)


def ensure_household_entities(session: Session, *, tax_year: int) -> list[HouseholdEntity]:
    existing = session.query(HouseholdEntity).filter(HouseholdEntity.tax_year == int(tax_year)).all()
    existing_pairs = {(str(row.entity_type), str(row.display_name)) for row in existing}
    created = False
    for entity_type, name in DEFAULT_HOUSEHOLD_ENTITIES:
        if (entity_type, name) in existing_pairs:
            continue
        session.add(
            HouseholdEntity(
                tax_year=int(tax_year),
                entity_type=entity_type,
                display_name=name,
            )
        )
        created = True
    if created:
        session.commit()
        existing = session.query(HouseholdEntity).filter(HouseholdEntity.tax_year == int(tax_year)).all()
    return existing


def list_household_entities(session: Session, *, tax_year: int) -> list[dict[str, Any]]:
    rows = ensure_household_entities(session, tax_year=int(tax_year))
    out = []
    for row in rows:
        out.append(
            {
                "id": row.id,
                "tax_year": row.tax_year,
                "entity_type": row.entity_type,
                "display_name": row.display_name,
                "tin_last4": row.tin_last4,
                "notes": row.notes,
            }
        )
    return out


def suggest_owner_entity_id(
    session: Session,
    *,
    tax_year: int,
    doc_type: str | None,
    extracted: dict[str, Any] | None = None,
) -> int | None:
    entities = ensure_household_entities(session, tax_year=int(tax_year))
    if not entities:
        return None
    doc_type_norm = (doc_type or "").upper()
    if doc_type_norm == "1095A":
        for ent in entities:
            if str(ent.entity_type) == "HOUSEHOLD":
                return ent.id

    extracted = extracted or {}
    fields = extracted.get("fields") or []
    meta = extracted.get("meta") or {}
    name_parts: list[str] = []
    for key in ("employee_name", "recipient_name", "payer_name", "employer_name", "entity_name"):
        val = _field_value(extracted, key)
        if val:
            name_parts.append(str(val))
    for key in ("recipient_name", "payer_name", "employer_name", "entity_name"):
        val = meta.get(key)
        if val:
            name_parts.append(str(val))
    text = _normalize_name(" ".join(name_parts))

    trust_entity = None
    user_entity = None
    for ent in entities:
        ent_type = str(ent.entity_type)
        if ent_type == "TRUST":
            trust_entity = ent
        if ent_type == "USER":
            user_entity = ent
        name_norm = _normalize_name(ent.display_name)
        if name_norm and name_norm in text:
            return ent.id

    if trust_entity and ("TRUST" in text or doc_type_norm == "K1"):
        return trust_entity.id
    if doc_type_norm in {"W2", "1099R", "1099INT", "1099DIV", "1099B"} and user_entity:
        return user_entity.id
    return entities[0].id if entities else None


def pdf_page_count(path: Path) -> int:
    try:
        import pdfplumber

        with pdfplumber.open(path) as pdf:
            return len(pdf.pages)
    except Exception:
        pass
    try:
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        return len(reader.pages)
    except Exception:
        return 0


def extract_pdf_text(path: Path, *, force_ocr: bool = False) -> tuple[list[str], list[bool], list[str]]:
    texts: list[str] = []
    ocr_used: list[bool] = []
    warnings: list[str] = []
    page_count = 0

    def _read_with_pdfplumber() -> None:
        nonlocal page_count
        import pdfplumber

        with pdfplumber.open(path) as pdf:
            page_count = len(pdf.pages)
            for p in pdf.pages:
                txt = p.extract_text() or ""
                texts.append(txt)

    def _read_with_pypdf() -> None:
        nonlocal page_count
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        page_count = len(reader.pages)
        for p in reader.pages:
            txt = p.extract_text() or ""
            texts.append(txt)

    try:
        _read_with_pdfplumber()
    except Exception:
        try:
            _read_with_pypdf()
        except Exception as exc:
            warnings.append(f"PDF text extraction failed: {exc}")
            return [""], [False], warnings

    ocr_targets = []
    for idx, text in enumerate(texts):
        if force_ocr or len((text or "").strip()) < MIN_TEXT_CHARS:
            ocr_targets.append(idx)
    ocr_used = [False] * len(texts)
    if ocr_targets:
        try:
            from pdf2image import convert_from_path
            import pytesseract
        except Exception as exc:
            warnings.append(f"OCR not available: {exc}")
            return texts, ocr_used, warnings

        for idx in ocr_targets:
            try:
                images = convert_from_path(str(path), first_page=idx + 1, last_page=idx + 1)
                if images:
                    ocr_text = pytesseract.image_to_string(images[0])
                    if ocr_text:
                        texts[idx] = ocr_text
                        ocr_used[idx] = True
            except Exception as exc:
                warnings.append(f"OCR failed on page {idx + 1}: {exc}")
    return texts, ocr_used, warnings


def detect_doc_type(text: str) -> str:
    t = (text or "").upper()
    if "FORM W-2" in t or "WAGE AND TAX STATEMENT" in t:
        return "W2"
    if "FORM 1099-INT" in t:
        return "1099INT"
    if "FORM 1099-DIV" in t:
        return "1099DIV"
    if "FORM 1099-B" in t:
        return "1099B"
    if "FORM 1099-R" in t:
        return "1099R"
    if "FORM 1095-A" in t:
        return "1095A"
    if "SCHEDULE K-1" in t:
        return "K1"
    if "FORM 1098" in t:
        return "1098"
    if "SSA-1099" in t:
        return "SSA1099"
    return "OTHER"


def parse_w2(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []

    wages = _find_amount_with_fallback(lines, ["WAGES", "COMPENSATION"], min_value=10.0, lookahead=3)
    federal_wh = _find_amount_with_fallback(lines, ["FEDERAL", "INCOME", "TAX", "WITHHELD"], min_value=10.0, lookahead=3)
    ss_wages = _find_amount_with_fallback(lines, ["SOCIAL", "SECURITY", "WAGES"], min_value=10.0, lookahead=3)
    ss_wh = _find_amount_with_fallback(lines, ["SOCIAL", "SECURITY", "TAX", "WITHHELD"], min_value=10.0, lookahead=3)
    med_wages = _find_amount_with_fallback(lines, ["MEDICARE", "WAGES"], min_value=10.0, lookahead=3)
    med_wh = _find_amount_with_fallback(lines, ["MEDICARE", "TAX", "WITHHELD"], min_value=10.0, lookahead=3)

    employer = _find_name_after(lines, ["EMPLOYER", "NAME"]) or ""
    employee = _find_name_after(lines, ["EMPLOYEE", "NAME"]) or ""
    ein_match = _EIN_RE.search(text or "")
    ein = ein_match.group(0) if ein_match else None

    def _add(key: str, label: str, value: Any, confidence: float) -> None:
        if value is None or value == "":
            return
        fields.append({"key": key, "label": label, "value": value, "confidence": confidence})

    _add("wages", "Wages (Box 1)", wages, 0.85 if wages is not None else 0.0)
    _add("federal_withholding", "Federal withholding (Box 2)", federal_wh, 0.85 if federal_wh is not None else 0.0)
    _add("ss_wages", "Social Security wages (Box 3)", ss_wages, 0.7 if ss_wages is not None else 0.0)
    _add("ss_withholding", "Social Security tax withheld (Box 4)", ss_wh, 0.7 if ss_wh is not None else 0.0)
    _add("medicare_wages", "Medicare wages (Box 5)", med_wages, 0.7 if med_wages is not None else 0.0)
    _add("medicare_withholding", "Medicare tax withheld (Box 6)", med_wh, 0.7 if med_wh is not None else 0.0)
    _add("employer_name", "Employer name", employer, 0.6 if employer else 0.0)
    _add("employee_name", "Employee name", employee, 0.6 if employee else 0.0)
    if ein:
        _add("employer_ein", "Employer EIN", ein, 0.6)

    if wages is None:
        warnings.append("Missing Box 1 wages.")
    if federal_wh is None:
        warnings.append("Missing Box 2 federal withholding.")

    meta = {"employer_name": employer, "employee_name": employee, "employer_ein": ein}
    return fields, meta, warnings


def parse_1099int(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []
    interest = _find_amount(lines, ["INTEREST", "INCOME"])
    payer = _find_name_after(lines, ["PAYER'S", "NAME"]) or _find_name_after(lines, ["PAYER"]) or ""

    if interest is not None:
        fields.append({"key": "interest_income", "label": "Interest income (Box 1)", "value": interest, "confidence": 0.85})
    else:
        warnings.append("Missing Box 1 interest income.")
    if payer:
        fields.append({"key": "payer_name", "label": "Payer", "value": payer, "confidence": 0.6})
    return fields, {"payer_name": payer}, warnings


def parse_1099div(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []
    ordinary = _find_amount(lines, ["ORDINARY", "DIVIDENDS"])
    qualified = _find_amount(lines, ["QUALIFIED", "DIVIDENDS"])
    cap_gain = _find_amount(lines, ["CAPITAL", "GAIN", "DISTRIBUTIONS"])
    payer = _find_name_after(lines, ["PAYER'S", "NAME"]) or _find_name_after(lines, ["PAYER"]) or ""

    if ordinary is not None:
        fields.append({"key": "ordinary_dividends", "label": "Ordinary dividends (Box 1a)", "value": ordinary, "confidence": 0.85})
    else:
        warnings.append("Missing Box 1a ordinary dividends.")
    if qualified is not None:
        fields.append({"key": "qualified_dividends", "label": "Qualified dividends (Box 1b)", "value": qualified, "confidence": 0.8})
    if cap_gain is not None:
        fields.append({"key": "cap_gain_dist", "label": "Capital gain distributions (Box 2a)", "value": cap_gain, "confidence": 0.8})
    if payer:
        fields.append({"key": "payer_name", "label": "Payer", "value": payer, "confidence": 0.6})
    return fields, {"payer_name": payer}, warnings


def parse_1099r(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []
    gross = _find_amount(lines, ["GROSS", "DISTRIBUTION"])
    taxable = _find_amount(lines, ["TAXABLE", "AMOUNT"])
    federal_wh = _find_amount(lines, ["FEDERAL", "INCOME", "TAX", "WITHHELD"])
    payer = _find_name_after(lines, ["PAYER'S", "NAME"]) or _find_name_after(lines, ["PAYER"]) or ""

    if gross is not None:
        fields.append({"key": "gross_distribution", "label": "Gross distribution (Box 1)", "value": gross, "confidence": 0.85})
    else:
        warnings.append("Missing Box 1 gross distribution.")
    if taxable is not None:
        fields.append({"key": "taxable_amount", "label": "Taxable amount (Box 2a)", "value": taxable, "confidence": 0.8})
    else:
        warnings.append("Missing Box 2a taxable amount (will assume taxable).")
    if federal_wh is not None:
        fields.append({"key": "federal_withholding", "label": "Federal withholding (Box 4)", "value": federal_wh, "confidence": 0.85})
    if payer:
        fields.append({"key": "payer_name", "label": "Payer", "value": payer, "confidence": 0.6})
    return fields, {"payer_name": payer}, warnings


def parse_1099b(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []
    proceeds = _find_amount(lines, ["PROCEEDS"])
    basis = _find_amount(lines, ["COST", "BASIS"])
    gain = _find_amount(lines, ["GAIN", "OR", "LOSS"])
    payer = _find_name_after(lines, ["PAYER'S", "NAME"]) or _find_name_after(lines, ["PAYER"]) or _find_name_after(lines, ["BROKER"]) or ""

    if proceeds is not None:
        fields.append({"key": "proceeds", "label": "Proceeds", "value": proceeds, "confidence": 0.7})
    if basis is not None:
        fields.append({"key": "cost_basis", "label": "Cost basis", "value": basis, "confidence": 0.7})
    if gain is not None:
        fields.append({"key": "gain_loss", "label": "Gain/loss", "value": gain, "confidence": 0.6})
    if not any([proceeds, basis, gain]):
        warnings.append("No summary totals detected on 1099-B.")
    if payer:
        fields.append({"key": "payer_name", "label": "Payer", "value": payer, "confidence": 0.6})
    return fields, {"payer_name": payer}, warnings


def parse_k1(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []

    def _find(label_tokens: list[str], key: str, label: str) -> None:
        amt = _find_amount(lines, label_tokens)
        if amt is not None:
            fields.append({"key": key, "label": label, "value": amt, "confidence": 0.7})

    _find(["ORDINARY", "BUSINESS", "INCOME"], "k1_ordinary_income", "Ordinary business income (K-1)")
    _find(["INTEREST", "INCOME"], "k1_interest", "Interest income (K-1)")
    _find(["DIVIDENDS"], "k1_dividends", "Dividends (K-1)")
    _find(["NET", "RENTAL"], "k1_rental", "Net rental real estate income (K-1)")

    entity = _find_name_after(lines, ["PARTNERSHIP", "S", "NAME"]) or _find_name_after(lines, ["ENTITY", "NAME"]) or ""
    recipient = _find_name_after(lines, ["PARTNER", "S", "NAME"]) or _find_name_after(lines, ["BENEFICIARY", "NAME"]) or ""
    ein_match = _EIN_RE.search(text or "")
    ein = ein_match.group(0) if ein_match else None

    if entity:
        fields.append({"key": "entity_name", "label": "Entity name", "value": entity, "confidence": 0.6})
    if recipient:
        fields.append({"key": "recipient_name", "label": "Recipient name", "value": recipient, "confidence": 0.6})
    if ein:
        fields.append({"key": "entity_ein", "label": "Entity EIN", "value": ein, "confidence": 0.6})
    if not any(f["key"].startswith("k1_") for f in fields):
        warnings.append("No common K-1 income lines detected.")
    return fields, {"payer_name": entity, "recipient_name": recipient, "entity_ein": ein}, warnings


def parse_1095a(text: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    lines = _normalize_lines(text)
    warnings: list[str] = []
    fields: list[dict[str, Any]] = []
    premiums_total = None
    slcsp_total = None
    aptc_total = None
    monthly: dict[str, list[float]] = {"premium": [], "slcsp": [], "aptc": []}

    for line in lines:
        if "ANNUAL" in line and "TOTAL" in line:
            nums = _extract_amounts(line)
            if len(nums) >= 3:
                premiums_total, slcsp_total, aptc_total = nums[:3]
                break

    if premiums_total is None:
        for line in lines:
            if any(m in line for m in _MONTHS):
                nums = _extract_amounts(line)
                if len(nums) >= 3:
                    monthly["premium"].append(nums[0])
                    monthly["slcsp"].append(nums[1])
                    monthly["aptc"].append(nums[2])
    if monthly["premium"]:
        premiums_total = sum(monthly["premium"])
        slcsp_total = sum(monthly["slcsp"])
        aptc_total = sum(monthly["aptc"])

    if premiums_total is not None:
        fields.append({"key": "aca_premium_total", "label": "ACA premiums (1095-A)", "value": premiums_total, "confidence": 0.8})
    else:
        warnings.append("Missing ACA premium totals.")
    if slcsp_total is not None:
        fields.append({"key": "aca_slcsp_total", "label": "ACA SLCSP total (1095-A)", "value": slcsp_total, "confidence": 0.8})
    if aptc_total is not None:
        fields.append({"key": "aca_aptc_total", "label": "ACA APTC total (1095-A)", "value": aptc_total, "confidence": 0.8})

    return fields, {"monthly": monthly}, warnings


PARSERS = {
    "W2": parse_w2,
    "1099INT": parse_1099int,
    "1099DIV": parse_1099div,
    "1099R": parse_1099r,
    "1099B": parse_1099b,
    "K1": parse_k1,
    "1095A": parse_1095a,
}


def extract_tax_document(path: Path, *, doc_type_hint: str | None = None, force_ocr: bool = False) -> dict[str, Any]:
    texts, ocr_used, warnings = extract_pdf_text(path, force_ocr=force_ocr)
    full_text = "\n".join(texts)
    detected = detect_doc_type(full_text)
    doc_type = doc_type_hint or detected
    parser = PARSERS.get(doc_type)
    fields: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    parser_warnings: list[str] = []
    if parser:
        fields, meta, parser_warnings = parser(full_text)
    else:
        parser_warnings.append("No parser available for this document type.")

    # If extraction looks empty or suspicious, retry with OCR.
    if parser and not force_ocr:
        def _field_amount(key: str) -> float | None:
            for f in fields:
                if f.get("key") == key:
                    val = f.get("value")
                    return _parse_amount(val) if isinstance(val, str) else float(val) if val is not None else None
            return None

        suspicious = False
        if not fields:
            suspicious = True
        elif doc_type == "W2":
            wages_val = _field_amount("wages")
            if wages_val is not None and wages_val <= 10.0:
                suspicious = True
        if suspicious:
            ocr_texts, ocr_used2, ocr_warnings = extract_pdf_text(path, force_ocr=True)
            full_text2 = "\n".join(ocr_texts)
            detected2 = detect_doc_type(full_text2)
            doc_type2 = doc_type_hint or detected2 or doc_type
            parser2 = PARSERS.get(doc_type2)
            if parser2:
                fields2, meta2, parser_warnings2 = parser2(full_text2)
                if fields2:
                    fields = fields2
                    meta = meta2
                    parser_warnings = parser_warnings2
                    full_text = full_text2
                    detected = detected2
                    ocr_used = ocr_used2
                    warnings.extend(ocr_warnings)
                    warnings.append("OCR retry applied for low-confidence extraction.")

    warnings.extend(parser_warnings)
    corrected = "CORRECTED" in full_text.upper()
    confidence_vals = [float(f.get("confidence") or 0.0) for f in fields if f.get("value") not in (None, "")]
    confidence_overall = sum(confidence_vals) / len(confidence_vals) if confidence_vals else 0.0

    meta = meta or {}
    meta["detected_doc_type"] = detected
    meta["ocr_used"] = ocr_used
    meta["corrected"] = corrected
    meta["text_length"] = len(full_text or "")
    meta["page_text_lengths"] = [len(t or "") for t in texts]

    extraction = {
        "doc_type": doc_type,
        "fields": fields,
        "meta": meta,
        "warnings": warnings,
        "confidence_overall": confidence_overall,
    }
    return extraction


def latest_extraction(session: Session, *, doc_id: int) -> TaxDocumentExtraction | None:
    return (
        session.query(TaxDocumentExtraction)
        .filter(TaxDocumentExtraction.tax_document_id == doc_id)
        .order_by(TaxDocumentExtraction.extracted_at.desc(), TaxDocumentExtraction.id.desc())
        .first()
    )


def _field_value(extracted: dict[str, Any], key: str) -> Any:
    for f in extracted.get("fields") or []:
        if f.get("key") == key:
            if f.get("value_confirmed") not in (None, ""):
                return f.get("value_confirmed")
            return f.get("value")
    return None


def build_facts_from_extraction(
    doc: TaxDocument,
    extracted: dict[str, Any],
    *,
    owner_overrides: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    doc_type = doc.doc_type if doc.doc_type and doc.doc_type != "OTHER" else extracted.get("doc_type") or "OTHER"
    fields = extracted.get("fields") or []
    meta = extracted.get("meta") or {}

    def _num(key: str) -> float | None:
        val = _field_value(extracted, key)
        return _parse_amount(val) if isinstance(val, str) else float(val) if val is not None else None

    payer_name = _field_value(extracted, "payer_name") or meta.get("payer_name") or meta.get("employer_name") or ""
    recipient_name = _field_value(extracted, "employee_name") or _field_value(extracted, "recipient_name") or meta.get("recipient_name") or ""

    facts: list[dict[str, Any]] = []

    def _add_fact(fact_type: str, amount: float | None, extra: dict[str, Any] | None = None) -> None:
        if amount is None:
            return
        owner_id = doc.owner_entity_id
        if owner_overrides and fact_type in owner_overrides:
            owner_id = owner_overrides.get(fact_type)
        facts.append(
            {
                "tax_year": doc.tax_year,
                "source_doc_id": doc.id,
                "fact_type": fact_type,
                "payer_name": payer_name or None,
                "recipient_name": recipient_name or None,
                "amount": float(amount),
                "metadata_json": extra or {},
                "confidence": extracted.get("confidence_overall"),
                "user_confirmed": True,
                "owner_entity_id": owner_id,
            }
        )

    if doc_type == "W2":
        _add_fact("WAGES", _num("wages"), {"box": "1"})
        _add_fact("FED_WITHHOLDING", _num("federal_withholding"), {"box": "2"})
    elif doc_type == "1099INT":
        _add_fact("INT_INCOME", _num("interest_income"), {"box": "1"})
    elif doc_type == "1099DIV":
        _add_fact("DIV_ORDINARY", _num("ordinary_dividends"), {"box": "1a"})
        _add_fact("DIV_QUALIFIED", _num("qualified_dividends"), {"box": "1b"})
        _add_fact("CAP_GAIN_DIST", _num("cap_gain_dist"), {"box": "2a"})
    elif doc_type == "1099R":
        taxable = _num("taxable_amount")
        gross = _num("gross_distribution")
        meta = {"box": "2a" if taxable is not None else "1"}
        if gross is not None:
            meta["gross_distribution"] = float(gross)
        _add_fact("IRA_DISTRIBUTION", taxable if taxable is not None else gross, meta)
        _add_fact("IRA_WITHHOLDING", _num("federal_withholding"), {"box": "4"})
    elif doc_type == "1099B":
        _add_fact("B_PROCEEDS", _num("proceeds"), {})
        _add_fact("B_COST_BASIS", _num("cost_basis"), {})
        _add_fact("B_GAIN_LOSS", _num("gain_loss"), {})
    elif doc_type == "K1":
        _add_fact("K1_ORD_INCOME", _num("k1_ordinary_income"), {})
        _add_fact("K1_INTEREST", _num("k1_interest"), {})
        _add_fact("K1_DIVIDENDS", _num("k1_dividends"), {})
        _add_fact("K1_RENTAL", _num("k1_rental"), {})
    elif doc_type == "1095A":
        monthly = meta.get("monthly") or {}
        _add_fact("ACA_PREMIUM", _num("aca_premium_total"), {"monthly": monthly.get("premium")})
        _add_fact("ACA_SLCSP", _num("aca_slcsp_total"), {"monthly": monthly.get("slcsp")})
        _add_fact("ACA_APTC", _num("aca_aptc_total"), {"monthly": monthly.get("aptc")})

    return facts


def _doc_is_authoritative(doc: TaxDocument) -> bool:
    if doc.is_authoritative is True:
        return True
    if doc.is_corrected:
        return False
    if doc.is_authoritative is False:
        return False
    return True


def aggregate_tax_doc_overrides(session: Session, *, tax_year: int) -> dict[str, Any]:
    rows = (
        session.query(TaxFact, TaxDocument)
        .join(TaxDocument, TaxDocument.id == TaxFact.source_doc_id)
        .filter(TaxFact.tax_year == int(tax_year), TaxFact.user_confirmed.is_(True))
        .all()
    )
    totals = {
        "w2_wages_total": 0.0,
        "w2_withholding_total": 0.0,
        "ira_distributions_total": 0.0,
        "ira_distributions_gross_total": 0.0,
        "ira_withholding_total": 0.0,
        "interest_total": 0.0,
        "dividends_ordinary_total": 0.0,
        "dividends_qualified_total": 0.0,
        "cap_gain_dist_total": 0.0,
        "k1_ordinary_total": 0.0,
        "k1_interest_total": 0.0,
        "k1_dividends_total": 0.0,
        "k1_rental_total": 0.0,
        "k1_other_total": 0.0,
        "aca_premium_monthly": [0.0] * 12,
        "aca_aptc_monthly": [0.0] * 12,
        "aca_slcsp_monthly": [0.0] * 12,
        "sources": {},
        "by_entity": {},
    }
    for fact, doc in rows:
        if not _doc_is_authoritative(doc):
            continue
        totals["sources"].setdefault(fact.fact_type, set()).add(int(fact.source_doc_id))
        amt = float(fact.amount or 0.0)
        owner_id = int(fact.owner_entity_id or doc.owner_entity_id or 0)
        if owner_id:
            ent_totals = totals["by_entity"].setdefault(
                owner_id,
                {
                    "owner_entity_id": owner_id,
                    "w2_wages_total": 0.0,
                    "w2_withholding_total": 0.0,
                    "ira_distributions_total": 0.0,
                    "ira_withholding_total": 0.0,
                    "interest_total": 0.0,
                    "dividends_ordinary_total": 0.0,
                    "dividends_qualified_total": 0.0,
                    "cap_gain_dist_total": 0.0,
                    "k1_ordinary_total": 0.0,
                    "k1_interest_total": 0.0,
                    "k1_dividends_total": 0.0,
                    "k1_rental_total": 0.0,
                    "k1_other_total": 0.0,
                    "aca_premium_total": 0.0,
                    "aca_aptc_total": 0.0,
                    "aca_slcsp_total": 0.0,
                },
            )
        if fact.fact_type == "WAGES":
            totals["w2_wages_total"] += amt
            if owner_id:
                ent_totals["w2_wages_total"] += amt
        elif fact.fact_type == "FED_WITHHOLDING":
            totals["w2_withholding_total"] += amt
            if owner_id:
                ent_totals["w2_withholding_total"] += amt
        elif fact.fact_type == "IRA_DISTRIBUTION":
            totals["ira_distributions_total"] += amt
            gross_meta = (fact.metadata_json or {}).get("gross_distribution")
            if gross_meta is not None:
                totals["ira_distributions_gross_total"] += float(gross_meta)
            if owner_id:
                ent_totals["ira_distributions_total"] += amt
        elif fact.fact_type == "IRA_WITHHOLDING":
            totals["ira_withholding_total"] += amt
            if owner_id:
                ent_totals["ira_withholding_total"] += amt
        elif fact.fact_type == "INT_INCOME":
            totals["interest_total"] += amt
            if owner_id:
                ent_totals["interest_total"] += amt
        elif fact.fact_type == "DIV_ORDINARY":
            totals["dividends_ordinary_total"] += amt
            if owner_id:
                ent_totals["dividends_ordinary_total"] += amt
        elif fact.fact_type == "DIV_QUALIFIED":
            totals["dividends_qualified_total"] += amt
            if owner_id:
                ent_totals["dividends_qualified_total"] += amt
        elif fact.fact_type == "CAP_GAIN_DIST":
            totals["cap_gain_dist_total"] += amt
            if owner_id:
                ent_totals["cap_gain_dist_total"] += amt
        elif fact.fact_type == "K1_ORD_INCOME":
            totals["k1_ordinary_total"] += amt
            if owner_id:
                ent_totals["k1_ordinary_total"] += amt
        elif fact.fact_type == "K1_INTEREST":
            totals["k1_interest_total"] += amt
            if owner_id:
                ent_totals["k1_interest_total"] += amt
        elif fact.fact_type == "K1_DIVIDENDS":
            totals["k1_dividends_total"] += amt
            if owner_id:
                ent_totals["k1_dividends_total"] += amt
        elif fact.fact_type == "K1_RENTAL":
            totals["k1_rental_total"] += amt
            if owner_id:
                ent_totals["k1_rental_total"] += amt
        elif fact.fact_type == "K1_OTHER":
            totals["k1_other_total"] += amt
            if owner_id:
                ent_totals["k1_other_total"] += amt
        elif fact.fact_type == "ACA_PREMIUM":
            monthly = (fact.metadata_json or {}).get("monthly") or []
            if monthly and len(monthly) >= 12:
                totals["aca_premium_monthly"] = [float(v or 0.0) for v in monthly[:12]]
            else:
                totals["aca_premium_monthly"] = [amt / 12.0] * 12
            if owner_id:
                ent_totals["aca_premium_total"] += amt
        elif fact.fact_type == "ACA_APTC":
            monthly = (fact.metadata_json or {}).get("monthly") or []
            if monthly and len(monthly) >= 12:
                totals["aca_aptc_monthly"] = [float(v or 0.0) for v in monthly[:12]]
            else:
                totals["aca_aptc_monthly"] = [amt / 12.0] * 12
            if owner_id:
                ent_totals["aca_aptc_total"] += amt
        elif fact.fact_type == "ACA_SLCSP":
            monthly = (fact.metadata_json or {}).get("monthly") or []
            if monthly and len(monthly) >= 12:
                totals["aca_slcsp_monthly"] = [float(v or 0.0) for v in monthly[:12]]
            else:
                totals["aca_slcsp_monthly"] = [amt / 12.0] * 12
            if owner_id:
                ent_totals["aca_slcsp_total"] += amt

    totals["sources"] = {k: sorted(list(v)) for k, v in totals["sources"].items()}
    if totals["ira_distributions_gross_total"] == 0.0:
        totals["ira_distributions_gross_total"] = totals["ira_distributions_total"]
    return totals


def tax_docs_summary(session: Session, *, tax_year: int) -> list[dict[str, Any]]:
    entities = session.query(HouseholdEntity).filter(HouseholdEntity.tax_year == int(tax_year)).all()
    owner_map = {int(e.id): {"display_name": e.display_name, "entity_type": e.entity_type} for e in entities}
    docs = (
        session.query(TaxDocument)
        .filter(TaxDocument.tax_year == int(tax_year))
        .order_by(TaxDocument.uploaded_at.desc(), TaxDocument.id.desc())
        .all()
    )
    facts = (
        session.query(TaxFact)
        .filter(TaxFact.tax_year == int(tax_year))
        .all()
    )
    by_doc: dict[int, list[TaxFact]] = {}
    for fact in facts:
        by_doc.setdefault(int(fact.source_doc_id), []).append(fact)

    out: list[dict[str, Any]] = []
    for doc in docs:
        doc_facts = by_doc.get(int(doc.id), [])
        totals: dict[str, float] = {}
        payer = None
        for fact in doc_facts:
            totals[fact.fact_type] = totals.get(fact.fact_type, 0.0) + float(fact.amount or 0.0)
            if not payer and fact.payer_name:
                payer = fact.payer_name
        out.append(
            {
                "id": doc.id,
                "doc_type": doc.doc_type,
                "filename": doc.filename,
                "status": doc.status,
                "uploaded_at": doc.uploaded_at,
                "payer_name": payer,
                "is_corrected": bool(doc.is_corrected),
                "is_authoritative": bool(doc.is_authoritative) if doc.is_authoritative is not None else None,
                "owner_entity_id": doc.owner_entity_id,
                "owner_label": owner_map.get(int(doc.owner_entity_id or 0), {}).get("display_name"),
                "owner_type": owner_map.get(int(doc.owner_entity_id or 0), {}).get("entity_type"),
                "totals": totals,
            }
        )
    return out


def build_tax_reconciliation(session: Session, *, tax_year: int) -> dict[str, Any]:
    from src.core.taxes import build_tax_dashboard

    year = int(tax_year)
    overrides = aggregate_tax_doc_overrides(session, tax_year=year)
    as_of = dt.date(year, 12, 31)
    dashboard = build_tax_dashboard(session, year=year, as_of=as_of, apply_overrides=True)
    investor = build_tax_dashboard(session, year=year, as_of=as_of, apply_overrides=False)

    sources = (dashboard.summary or {}).get("sources") or {}
    docs_present = bool((dashboard.summary or {}).get("docs_present"))
    docs_primary = bool((dashboard.summary or {}).get("docs_primary"))

    categories = [
        ("w2_wages_total", "Wages (W-2)", overrides.get("w2_wages_total"), investor.summary["ordinary_breakdown"]["w2_wages"]),
        (
            "w2_withholding_total",
            "W-2 withholding",
            overrides.get("w2_withholding_total"),
            investor.summary.get("w2_withholding_ytd", 0.0),
        ),
        (
            "ira_distributions_total",
            "IRA distributions",
            overrides.get("ira_distributions_total"),
            investor.summary["ordinary_breakdown"]["ira_distributions"],
        ),
        (
            "ira_withholding_total",
            "IRA withholding",
            overrides.get("ira_withholding_total"),
            investor.summary.get("ira_withholding_ytd", 0.0),
        ),
        ("interest_total", "Interest income", overrides.get("interest_total"), investor.summary["ordinary_breakdown"]["interest"]),
        ("dividends_ordinary_total", "Dividends (ordinary)", overrides.get("dividends_ordinary_total"), investor.summary["ordinary_breakdown"]["dividends"]),
        ("cap_gain_dist_total", "Capital gain distributions", overrides.get("cap_gain_dist_total"), 0.0),
        ("k1_total", "K-1 income", sum([
            overrides.get("k1_ordinary_total") or 0.0,
            overrides.get("k1_interest_total") or 0.0,
            overrides.get("k1_dividends_total") or 0.0,
            overrides.get("k1_rental_total") or 0.0,
            overrides.get("k1_other_total") or 0.0,
        ]), investor.summary["ordinary_breakdown"].get("k1_income", 0.0)),
        (
            "aca_premium_monthly",
            "ACA premiums",
            sum(overrides.get("aca_premium_monthly") or []),
            float(investor.summary.get("aca", {}).get("premium_paid", 0.0)),
        ),
        ("aca_aptc_monthly", "ACA APTC", sum(overrides.get("aca_aptc_monthly") or []), investor.summary.get("aca", {}).get("aptc_received", 0.0)),
    ]

    rows = []
    for key, label, docs_total, investor_total in categories:
        docs_val = float(docs_total or 0.0)
        inv_val = float(investor_total or 0.0)
        rows.append(
            {
                "key": key,
                "label": label,
                "docs_total": docs_val,
                "investor_total": inv_val,
                "delta": docs_val - inv_val,
                "source_used": sources.get(key) or "investor",
            }
        )
    return {
        "tax_year": year,
        "docs_primary": docs_primary,
        "docs_present": docs_present,
        "rows": rows,
        "doc_ids": sorted({int(i) for ids in (overrides.get("sources") or {}).values() for i in ids}),
    }


def serialize_tax_document(doc: TaxDocument, extraction: TaxDocumentExtraction | None) -> dict[str, Any]:
    extracted_json = extraction.extracted_json if extraction else None
    if extracted_json and extracted_json.get("fields"):
        for field in extracted_json["fields"]:
            key = str(field.get("key") or "").lower()
            if key in {"employer_ein", "entity_ein"} or "ssn" in key or "ein" in key:
                field["value_masked"] = _mask_identifier(field.get("value"))
    custom_doc_type = _custom_doc_type_from_notes(doc.notes)
    return {
        "id": doc.id,
        "tax_year": doc.tax_year,
        "doc_type": doc.doc_type,
        "custom_doc_type": custom_doc_type,
        "filename": doc.filename,
        "uploaded_at": doc.uploaded_at.isoformat() if doc.uploaded_at else None,
        "sha256": doc.sha256,
        "status": doc.status,
        "notes": doc.notes,
        "page_count": doc.page_count,
        "is_corrected": bool(doc.is_corrected),
        "is_authoritative": bool(doc.is_authoritative) if doc.is_authoritative is not None else None,
        "owner_entity_id": doc.owner_entity_id,
        "extraction": {
            "confidence_overall": extraction.confidence_overall if extraction else None,
            "warnings": extraction.warnings if extraction else [],
            "extracted_json": extracted_json or {},
            "extracted_at": extraction.extracted_at.isoformat() if extraction else None,
            "extractor_version": extraction.extractor_version if extraction else None,
        },
    }


def save_tax_document(
    session: Session,
    *,
    tax_year: int,
    file_name: str,
    file_bytes: bytes,
    actor: str,
    doc_type: str | None = None,
) -> TaxDocument:
    safe_name = _safe_filename(file_name)
    file_hash = _sha256_bytes(file_bytes)
    existing = (
        session.query(TaxDocument)
        .filter(TaxDocument.tax_year == int(tax_year), TaxDocument.sha256 == file_hash)
        .one_or_none()
    )
    if existing:
        return existing

    dest_dir = DATA_DIR / str(tax_year)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{file_hash[:10]}_{safe_name}"
    dest.write_bytes(file_bytes)

    page_count = pdf_page_count(dest)
    corrected = "CORRECTED" in safe_name.upper()
    doc = TaxDocument(
        user_id=actor,
        household_id="household",
        tax_year=int(tax_year),
        doc_type=(doc_type if doc_type in DOC_TYPES else "OTHER"),
        filename=safe_name,
        sha256=file_hash,
        status="UPLOADED",
        raw_file_path=str(dest),
        page_count=page_count,
        is_corrected=corrected,
    )
    session.add(doc)
    session.commit()
    return doc
