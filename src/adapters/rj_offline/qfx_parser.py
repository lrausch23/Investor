from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Iterable


class QfxParseError(Exception):
    pass


_TAG_RE = re.compile(r"<(/?)([A-Za-z0-9_.:-]+)>", re.MULTILINE)


@dataclass
class OfxNode:
    name: str
    value: str | None = None
    children: dict[str, list["OfxNode"]] | None = None

    def add_child(self, node: "OfxNode") -> None:
        if self.children is None:
            self.children = {}
        self.children.setdefault(node.name, []).append(node)

    def first(self, name: str) -> "OfxNode | None":
        if not self.children:
            return None
        lst = self.children.get(name)
        return lst[0] if lst else None

    def all(self, name: str) -> list["OfxNode"]:
        if not self.children:
            return []
        return list(self.children.get(name) or [])


def _clean_text(s: str | None) -> str:
    if s is None:
        return ""
    # OFX/QFX values are often unescaped but can include stray newlines/spaces.
    return " ".join(str(s).replace("\x00", "").split()).strip()


def parse_ofx_sgml(text: str) -> OfxNode:
    """
    Parse OFX/QFX SGML-like content into a lightweight node tree.

    Quicken QFX files are often OFX 1.x style where leaf tags are of the form:
      <TAG>value
    Container tags are usually explicitly closed:
      <INVTRANLIST> ... </INVTRANLIST>
    This parser is tolerant: it treats tags with immediate text as leaf nodes.
    """
    s = text
    m = _TAG_RE.search(s)
    if not m:
        raise QfxParseError("No OFX tags found.")

    root = OfxNode("ROOT", children={})
    stack: list[OfxNode] = [root]

    i = 0
    while True:
        m = _TAG_RE.search(s, i)
        if not m:
            break
        is_end = bool(m.group(1))
        tag = str(m.group(2) or "").upper()
        i = m.end()
        if not tag:
            continue

        if is_end:
            # Pop to the matching tag if present.
            for j in range(len(stack) - 1, 0, -1):
                if stack[j].name == tag:
                    stack = stack[:j]
                    break
            continue

        # Leaf text is everything until the next "<".
        next_m = _TAG_RE.search(s, i)
        raw_val = s[i : (next_m.start() if next_m else len(s))]
        val = _clean_text(raw_val)
        if val:
            stack[-1].add_child(OfxNode(tag, value=val))
            i = i + len(raw_val)
            continue

        node = OfxNode(tag, children={})
        stack[-1].add_child(node)
        stack.append(node)

    return root


def _find_first_path(node: OfxNode, path: Iterable[str]) -> OfxNode | None:
    cur: OfxNode | None = node
    for seg in path:
        if cur is None:
            return None
        cur = cur.first(str(seg).upper())
    return cur


def _first_text(node: OfxNode | None, tag: str) -> str | None:
    if node is None:
        return None
    ch = node.first(tag.upper())
    if ch is None:
        return None
    return ch.value


_OFX_DT_RE = re.compile(r"^(\d{8})(\d{6})?")


def parse_ofx_datetime(raw: str | None) -> dt.datetime | None:
    """
    Parse OFX datetime like:
      20260107112509[-5:EST]
      20260107
    Returns tz-aware UTC datetime when possible; otherwise naive UTC assumed.
    """
    s = _clean_text(raw)
    if not s:
        return None
    m = _OFX_DT_RE.match(s)
    if not m:
        return None
    ymd = m.group(1)
    hms = m.group(2) or "000000"
    try:
        naive = dt.datetime.strptime(ymd + hms, "%Y%m%d%H%M%S")
    except Exception:
        try:
            d0 = dt.datetime.strptime(ymd, "%Y%m%d")
            naive = d0
        except Exception:
            return None
    # OFX timezone bracket is informational; treat naive as UTC for MVP.
    return naive.replace(tzinfo=dt.timezone.utc)


def parse_ofx_date(raw: str | None) -> dt.date | None:
    d = parse_ofx_datetime(raw)
    return d.date() if d is not None else None


@dataclass(frozen=True)
class QfxHeaderMeta:
    broker_id: str | None
    acct_id: str | None
    dt_start: dt.date | None
    dt_end: dt.date | None
    dt_asof: dt.date | None
    org: str | None
    fid: str | None
    intuid: str | None


def extract_qfx_header_meta(text: str) -> QfxHeaderMeta:
    root = parse_ofx_sgml(text)
    inv = _find_first_path(
        root,
        ["OFX", "INVSTMTMSGSRSV1", "INVSTMTTRNRS", "INVSTMTRS"],
    )
    broker_id = _first_text(_find_first_path(inv or root, ["INVACCTFROM"]), "BROKERID")
    acct_id = _first_text(_find_first_path(inv or root, ["INVACCTFROM"]), "ACCTID")
    tranlist = _find_first_path(inv or root, ["INVTRANLIST"])
    dt_start = parse_ofx_date(_first_text(tranlist, "DTSTART"))
    dt_end = parse_ofx_date(_first_text(tranlist, "DTEND"))
    dt_asof = parse_ofx_date(_first_text(inv, "DTASOF")) or parse_ofx_date(_first_text(_find_first_path(inv, ["INVPOSLIST"]), "DTASOF"))
    org = _first_text(_find_first_path(root, ["OFX", "SIGNONMSGSRSV1", "SONRS", "FI"]), "ORG")
    fid = _first_text(_find_first_path(root, ["OFX", "SIGNONMSGSRSV1", "SONRS", "FI"]), "FID")
    intuid = _first_text(_find_first_path(root, ["OFX", "INVSTMTMSGSRSV1", "INVSTMTTRNRS"]), "TRNUID")
    return QfxHeaderMeta(
        broker_id=_clean_text(broker_id) or None,
        acct_id=_clean_text(acct_id) or None,
        dt_start=dt_start,
        dt_end=dt_end,
        dt_asof=dt_asof,
        org=_clean_text(org) or None,
        fid=_clean_text(fid) or None,
        intuid=_clean_text(intuid) or None,
    )


@dataclass(frozen=True)
class QfxSecurity:
    unique_id: str | None  # CUSIP
    unique_id_type: str | None
    ticker: str | None
    name: str | None
    sec_type: str | None


def _iter_all_nodes(node: OfxNode) -> Iterable[OfxNode]:
    yield node
    if node.children:
        for lst in node.children.values():
            for ch in lst:
                yield from _iter_all_nodes(ch)


def parse_security_list(text: str) -> dict[str, QfxSecurity]:
    root = parse_ofx_sgml(text)
    out: dict[str, QfxSecurity] = {}
    seclist = _find_first_path(root, ["OFX", "SECLISTMSGSRSV1", "SECLIST"])
    if seclist is None:
        return out
    # The list contains STOCKINFO/MFINFO/DEBTINFO/OTHERINFO etc.
    for node in _iter_all_nodes(seclist):
        if node.name.endswith("INFO") and node.name != "SECINFO":
            secinfo = node.first("SECINFO") or node.first("SECID")
            # Some files put SECINFO directly under <STOCKINFO>.
            if secinfo is None and node.name == "SECINFO":
                secinfo = node
            if secinfo is None:
                continue
            secid = secinfo.first("SECID") if secinfo.name != "SECID" else secinfo
            uid = _first_text(secid, "UNIQUEID") if secid else None
            uid_type = _first_text(secid, "UNIQUEIDTYPE") if secid else None
            ticker = _first_text(secinfo, "TICKER") or _first_text(node, "TICKER")
            name = _first_text(secinfo, "SECNAME") or _first_text(secinfo, "NAME") or _first_text(node, "SECNAME")
            uid_u = _clean_text(uid).upper() if uid else None
            if not uid_u:
                continue
            out[uid_u] = QfxSecurity(
                unique_id=uid_u,
                unique_id_type=_clean_text(uid_type).upper() if uid_type else None,
                ticker=_clean_text(ticker).upper() if ticker else None,
                name=_clean_text(name) or None,
                sec_type=node.name,
            )
    return out


@dataclass(frozen=True)
class QfxPosition:
    unique_id: str | None
    ticker: str | None
    name: str | None
    qty: float | None
    unit_price: float | None
    market_value: float | None
    cost_basis: float | None
    price_asof: dt.date | None
    pos_type: str | None


def _as_float(raw: str | None) -> float | None:
    s = _clean_text(raw)
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


def parse_positions(text: str, *, securities: dict[str, QfxSecurity] | None = None) -> tuple[dt.date | None, list[QfxPosition], dict[str, Any]]:
    root = parse_ofx_sgml(text)
    inv = _find_first_path(root, ["OFX", "INVSTMTMSGSRSV1", "INVSTMTTRNRS", "INVSTMTRS"])
    if inv is None:
        return None, [], {}
    invpos = inv.first("INVPOSLIST")
    asof = parse_ofx_date(_first_text(inv, "DTASOF")) or (parse_ofx_date(_first_text(invpos, "DTASOF")) if invpos else None)
    secs = securities or {}
    items: list[QfxPosition] = []
    meta: dict[str, Any] = {}
    for pos_container in (invpos.children.values() if invpos and invpos.children else []):
        for pos_node in pos_container:
            # POSSTOCK/POSMF/POSDEBT/POSOTHER
            invpos_child = pos_node.first("INVPOS") or pos_node
            secid = invpos_child.first("SECID")
            uid = _clean_text(_first_text(secid, "UNIQUEID")).upper() if secid else None
            sec = secs.get(uid or "")
            ticker = sec.ticker if sec else None
            name = sec.name if sec else None
            qty = _as_float(_first_text(invpos_child, "UNITS")) or _as_float(_first_text(invpos_child, "HELD"))
            unit_price = _as_float(_first_text(invpos_child, "UNITPRICE"))
            mktval = _as_float(_first_text(invpos_child, "MKTVAL"))
            cost = _as_float(_first_text(invpos_child, "COSTBASIS"))
            price_asof = parse_ofx_date(_first_text(invpos_child, "DTPRICEASOF")) or None
            pos_type = _clean_text(_first_text(invpos_child, "POSTYPE")).upper() if _first_text(invpos_child, "POSTYPE") else None
            items.append(
                QfxPosition(
                    unique_id=uid,
                    ticker=ticker,
                    name=name,
                    qty=qty,
                    unit_price=unit_price,
                    market_value=mktval,
                    cost_basis=cost,
                    price_asof=price_asof,
                    pos_type=pos_type,
                )
            )
    # Try to find total value/cash if present.
    invbal = inv.first("INVBAL")
    if invbal:
        meta["avail_cash"] = _as_float(_first_text(invbal, "AVAILCASH"))
        meta["margin_balance"] = _as_float(_first_text(invbal, "MARGINBALANCE"))
    return asof, items, meta


@dataclass(frozen=True)
class QfxTransaction:
    fitid: str | None
    dt_trade: dt.date | None
    dt_posted: dt.date | None
    raw_type: str
    amount: float | None
    units: float | None
    unit_price: float | None
    commission: float | None
    fees: float | None
    unique_id: str | None
    memo: str | None
    name: str | None


def parse_transactions(text: str) -> list[QfxTransaction]:
    root = parse_ofx_sgml(text)
    inv = _find_first_path(root, ["OFX", "INVSTMTMSGSRSV1", "INVSTMTTRNRS", "INVSTMTRS"])
    if inv is None:
        return []
    out: list[QfxTransaction] = []

    invtranlist = inv.first("INVTRANLIST")
    if invtranlist and invtranlist.children:
        for raw_type, nodes in invtranlist.children.items():
            for node in nodes:
                # For investment txns, the core fields live under INVTRAN.
                invtran = node.first("INVTRAN") or node
                fitid = _first_text(invtran, "FITID")
                dt_trade = parse_ofx_date(_first_text(invtran, "DTTRADE"))
                dt_posted = parse_ofx_date(_first_text(invtran, "DTSETTLE")) or parse_ofx_date(_first_text(invtran, "DTPOSTED"))
                memo = _first_text(invtran, "MEMO")

                secid = node.first("SECID") or (node.first("INVBUY").first("SECID") if node.first("INVBUY") else None)  # type: ignore[union-attr]
                if secid is None and node.first("INVSELL"):
                    secid = node.first("INVSELL").first("SECID")  # type: ignore[union-attr]
                uid = _clean_text(_first_text(secid, "UNIQUEID")).upper() if secid else None

                amount = _as_float(_first_text(node, "TOTAL"))
                units = _as_float(_first_text(node, "UNITS"))
                unit_price = _as_float(_first_text(node, "UNITPRICE"))
                commission = _as_float(_first_text(node, "COMMISSION"))
                fees = _as_float(_first_text(node, "FEES"))
                name = _first_text(node, "SECNAME") or _first_text(node, "NAME")

                out.append(
                    QfxTransaction(
                        fitid=_clean_text(fitid) or None,
                        dt_trade=dt_trade,
                        dt_posted=dt_posted,
                        raw_type=raw_type,
                        amount=amount,
                        units=units,
                        unit_price=unit_price,
                        commission=commission,
                        fees=fees,
                        unique_id=uid,
                        memo=_clean_text(memo) or None,
                        name=_clean_text(name) or None,
                    )
                )

    # Bank-style transfers inside investment statement.
    banklist = inv.first("BANKTRANLIST")
    if banklist and banklist.children and "STMTTRN" in banklist.children:
        for node in banklist.children.get("STMTTRN") or []:
            fitid = _first_text(node, "FITID")
            dt_posted = parse_ofx_date(_first_text(node, "DTPOSTED"))
            trnamt = _as_float(_first_text(node, "TRNAMT"))
            name = _first_text(node, "NAME")
            memo = _first_text(node, "MEMO")
            out.append(
                QfxTransaction(
                    fitid=_clean_text(fitid) or None,
                    dt_trade=None,
                    dt_posted=dt_posted,
                    raw_type="BANKTRN",
                    amount=trnamt,
                    units=None,
                    unit_price=None,
                    commission=None,
                    fees=None,
                    unique_id=None,
                    memo=_clean_text(memo) or None,
                    name=_clean_text(name) or None,
                )
            )

    return out


def stable_txn_id_from_qfx(*, provider_account_id: str, tx: QfxTransaction) -> str:
    if tx.fitid:
        return f"RJ:FITID:{tx.fitid}"
    parts = [
        provider_account_id,
        (tx.dt_trade or tx.dt_posted or dt.date(1970, 1, 1)).isoformat(),
        str(tx.raw_type or ""),
        str(tx.unique_id or ""),
        str(tx.units or ""),
        str(tx.unit_price or ""),
        str(tx.amount or ""),
        str(tx.name or ""),
        str(tx.memo or ""),
    ]
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"RJ:HASH:{h}"


def placeholder_ticker_from_security(sec: QfxSecurity | None, *, unique_id: str | None) -> str:
    if sec and sec.ticker:
        return sec.ticker.upper()
    if unique_id:
        return f"CUSIP:{unique_id}"
    # Deterministic placeholder (rare).
    return "RJSEC:" + hashlib.sha256("UNKNOWN".encode("utf-8")).hexdigest()[:12]

