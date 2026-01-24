(() => {
  const dataSeed = window.__CASH_BILLS_DATA__ || { as_of: new Date().toISOString().slice(0, 10), bills: [], cash_accounts: [] };
  const balanceHints = Array.isArray(window.__CASH_BILLS_CARD_BALANCES__) ? window.__CASH_BILLS_CARD_BALANCES__ : [];
  const state = {
    asOfMode: "today",
    asOfDate: dataSeed.as_of || new Date().toISOString().slice(0, 10),
    scope: "personal",
    rangeDays: 30,
    status: "all",
    loading: true,
    error: null,
    billsError: null,
    billsWarning: null,
    cashError: null,
    data: null,
    recurringLoading: true,
    recurringError: null,
    recurringBills: [],
    recurringDueTotal: 0,
    recurringAllLoading: false,
    recurringAllError: null,
    recurringAllBills: [],
    cardRecurringLoading: true,
    cardRecurringError: null,
    cardRecurringCharges: [],
    cardRecurringAllLoading: false,
    cardRecurringAllError: null,
    cardRecurringAllCharges: [],
    financeLoading: true,
    financeError: null,
    financeRows: [],
    financeMonths: 12,
    financeModalOpen: false,
    financeModalLoading: false,
    financeModalError: null,
    financeModalRows: [],
    financeModalContext: null,
    depositsLoading: false,
    depositsError: null,
    depositsAccounts: [],
    depositsMonthly: [],
    depositsAccountId: null,
    depositsMonths: 6,
    depositsModalOpen: false,
    depositsModalLoading: false,
    depositsModalError: null,
    depositsModalRows: [],
    depositsModalContext: null,
    calendarMonthOffset: 0,
    calendarModalOpen: false,
    calendarModalContext: null,
    calendarModalItems: [],
    payOverModalOpen: false,
    payOverModalContext: null,
    cardSuggestionsLoading: false,
    cardSuggestionsError: null,
    cardSuggestions: [],
    cardRecentLoading: false,
    cardRecentError: null,
    cardRecentCharges: [],
    cardRecentAppleOnly: false,
    cardMemberFilter: "ALL",
    cardAccountFilter: "ALL",
    cardModalOpen: false,
    cardModalTab: "suggested",
    cardModalEdit: null,
    cardModalFocusId: null,
    recentLoading: false,
    recentError: null,
    recentCharges: [],
    suggestionsLoading: false,
    suggestionsError: null,
    suggestions: [],
    modalOpen: false,
    modalTab: "suggested",
    modalEdit: null,
    modalFocusBillId: null,
  };

  const els = {
    asOfSelect: document.getElementById("cashBillsAsOf"),
    customWrap: document.getElementById("cashBillsCustomWrap"),
    customDate: document.getElementById("cashBillsCustomDate"),
    scopeSelect: document.getElementById("cashBillsScope"),
    refreshBtn: document.getElementById("cashBillsRefresh"),
    context: document.getElementById("cashBillsContext"),
    kpis: document.getElementById("cashBillsKpis"),
    billsTable: document.getElementById("cashBillsBillsTable"),
    billsSummary: document.getElementById("cashBillsBillsSummary"),
    cashTable: document.getElementById("cashBillsCashTable"),
    cashSummary: document.getElementById("cashBillsCashSummary"),
    coverage: document.getElementById("cashBillsCoverage"),
    coverageTitle: document.getElementById("cashBillsCoverageTitle"),
    rangeChips: document.getElementById("cashBillsRangeChips"),
    statusChips: document.getElementById("cashBillsStatusChips"),
    billsTitle: document.getElementById("cashBillsBillsTitle"),
    authBanner: document.getElementById("cashBillsAuthBanner"),
    dismissBanner: document.getElementById("cashBillsDismissBanner"),
    recurringTable: document.getElementById("cashBillsRecurringTable"),
    recurringSummary: document.getElementById("cashBillsRecurringSummary"),
    manageBillsBtn: document.getElementById("cashBillsManageBills"),
    cardRecurringTable: document.getElementById("cashBillsCardRecurringTable"),
    cardRecurringSummary: document.getElementById("cashBillsCardRecurringSummary"),
    financeTable: document.getElementById("cashBillsFinanceTable"),
    financeSummary: document.getElementById("cashBillsFinanceSummary"),
    monthlyTotals: document.getElementById("cashBillsMonthlyTotals"),
    manageCardChargesBtn: document.getElementById("cashBillsManageCardCharges"),
    cardModal: document.getElementById("cashBillsManageCardModal"),
    cardModalContent: document.getElementById("cashBillsCardModalContent"),
    cardModalTabs: document.getElementById("cashBillsCardModalTabs"),
    cardModalFilters: document.getElementById("cashBillsCardModalFilters"),
    cardModalClose: document.getElementById("cashBillsCardModalClose"),
    cardModalRescan: document.getElementById("cashBillsCardModalRescan"),
    cardModalApplePay: document.getElementById("cashBillsCardModalApplePay"),
    modal: document.getElementById("cashBillsManageModal"),
    modalContent: document.getElementById("cashBillsModalContent"),
    modalTabs: document.getElementById("cashBillsModalTabs"),
    modalClose: document.getElementById("cashBillsModalClose"),
    modalRescan: document.getElementById("cashBillsModalRescan"),
    financeModal: document.getElementById("cashBillsFinanceModal"),
    financeModalContent: document.getElementById("cashBillsFinanceContent"),
    financeModalClose: document.getElementById("cashBillsFinanceClose"),
    financeModalTitle: document.getElementById("cashBillsFinanceTitle"),
    depositsModal: document.getElementById("cashBillsDepositsModal"),
    depositsModalContent: document.getElementById("cashBillsDepositsContent"),
    depositsModalClose: document.getElementById("cashBillsDepositsClose"),
    depositsModalTitle: document.getElementById("cashBillsDepositsTitle"),
    payOverModal: document.getElementById("cashBillsPayOverModal"),
    payOverModalContent: document.getElementById("cashBillsPayOverContent"),
    payOverModalClose: document.getElementById("cashBillsPayOverClose"),
    payOverModalTitle: document.getElementById("cashBillsPayOverTitle"),
    calendarModal: document.getElementById("cashBillsCalendarModal"),
    calendarModalContent: document.getElementById("cashBillsCalendarContent"),
    calendarModalClose: document.getElementById("cashBillsCalendarClose"),
    calendarModalTitle: document.getElementById("cashBillsCalendarTitle"),
  };

  const currency = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 });

  const formatCardLabel = (name, mask) => {
    const base = (name || "").trim();
    if (mask) {
      return `${base || "Card"} • ${mask}`;
    }
    return base || "—";
  };

  const formatCardBillLabel = (bill) => {
    if (!bill) return "Card";
    const issuer = (bill.issuer || "").trim();
    const name = (bill.card_name || "").trim();
    let label = [issuer, name].filter(Boolean).join(" ").trim();
    if (!label) label = "Card";
    if (bill.last4) label = `${label} • ${bill.last4}`;
    return label;
  };

  const formatPlaidAccountId = (value) => {
    if (!value) return "";
    const id = String(value);
    if (id.length <= 10) return id;
    return `${id.slice(0, 6)}…${id.slice(-4)}`;
  };

  const normalizeMemberKey = (value) => {
    const raw = (value || "").trim();
    if (!raw) return "";
    const cleaned = raw.replace(/[\s•.]*\d{3,6}\s*$/g, "").trim();
    return cleaned.toLowerCase();
  };

  const normalizeMemberDisplay = (value) => {
    const raw = (value || "").trim();
    if (!raw) return "";
    let cleaned = raw.replace(/[\s•.]*\d{3,6}\s*$/g, "").trim();
    if (!cleaned) return "";
    const lettersOnly = cleaned.replace(/[^A-Za-z]/g, "");
    if (lettersOnly && lettersOnly === lettersOnly.toUpperCase()) {
      cleaned = cleaned
        .toLowerCase()
        .replace(/\b\w/g, (match) => match.toUpperCase());
    }
    return cleaned;
  };

  const formatMemberLabel = (value) => normalizeMemberDisplay(value) || "—";

  const formatMemberCell = (value, source) => {
    const memberLabel = formatMemberLabel(value);
    if (memberLabel === "—") return "—";
    const sourceLine = source ? `<div class="cashbills-modal__cell-muted">${source}</div>` : "";
    return `<div>${memberLabel}</div>${sourceLine}`;
  };

  const normalizeAccountKey = (value) => {
    const raw = (value || "").toString().trim();
    return raw ? raw.toLowerCase() : "";
  };

  const accountKeyFromRow = (row) => {
    return (
      row.source_account_id ||
      row.account_id ||
      row.plaid_account_id ||
      row.source_account_mask ||
      ""
    ).toString();
  };

  const accountLabelFromRow = (row) => {
    const name = (row.source_account_name || row.account_name || "").trim();
    const mask = (row.source_account_mask || row.account_mask || "").trim();
    if (name && mask) return `${name} • ${mask}`;
    if (name) return name;
    if (mask) return `Card • ${mask}`;
    return "";
  };

  const formatMerchantLabel = (merchantDisplay, descriptionSample, fallbackName) => {
    const display = (merchantDisplay || "").trim();
    if (display && display.toLowerCase() !== "unknown") return display;
    const desc = (descriptionSample || "").trim();
    if (desc) return desc;
    const fallback = (fallbackName || "").trim();
    if (fallback && fallback.toLowerCase() !== "unknown") return fallback;
    return "—";
  };

  const formatChargeLabel = (name, merchantDisplay, descriptionSample) => {
    const raw = (name || "").trim();
    if (raw && raw.toLowerCase() !== "unknown") return raw;
    return formatMerchantLabel(merchantDisplay, descriptionSample, "");
  };

  const collectMemberOptions = (rows) => {
    const options = new Map();
    let hasUnknown = false;
    rows.forEach((row) => {
      const display = normalizeMemberDisplay(row.cardholder_name);
      if (!display) {
        hasUnknown = true;
        return;
      }
      const key = normalizeMemberKey(display);
      if (!key) {
        hasUnknown = true;
        return;
      }
      if (!options.has(key)) {
        options.set(key, display);
      } else {
        const existing = options.get(key) || "";
        const existingMixed = /[a-z]/.test(existing) && /[A-Z]/.test(existing);
        const candidateMixed = /[a-z]/.test(display) && /[A-Z]/.test(display);
        if (!existingMixed && candidateMixed) {
          options.set(key, display);
        }
      }
    });
    return {
      options: Array.from(options.entries())
        .map(([key, label]) => ({ key, label }))
        .sort((a, b) => a.label.localeCompare(b.label)),
      hasUnknown,
    };
  };

  const collectAccountOptions = (rows) => {
    const options = new Map();
    let hasUnknown = false;
    rows.forEach((row) => {
      const keyRaw = accountKeyFromRow(row);
      const key = normalizeAccountKey(keyRaw);
      if (!key) {
        hasUnknown = true;
        return;
      }
      const label = accountLabelFromRow(row) || `Acct ${formatPlaidAccountId(keyRaw)}`;
      if (!options.has(key)) {
        options.set(key, label);
      }
    });
    return {
      options: Array.from(options.entries())
        .map(([key, label]) => ({ key, label }))
        .sort((a, b) => a.label.localeCompare(b.label)),
      hasUnknown,
    };
  };

  const matchesMemberFilter = (row) => {
    const filter = state.cardMemberFilter || "ALL";
    if (filter === "ALL") return true;
    const name = normalizeMemberKey(row.cardholder_name);
    if (filter === "UNKNOWN") {
      return !name;
    }
    return name === filter;
  };

  const matchesAccountFilter = (row) => {
    const filter = state.cardAccountFilter || "ALL";
    if (filter === "ALL") return true;
    const key = normalizeAccountKey(accountKeyFromRow(row));
    if (filter === "UNKNOWN") {
      return !key;
    }
    return key === filter;
  };

  const isApplePayCharge = (row) => {
    const candidates = [row.description_sample, row.merchant_display, row.name]
      .map((value) => (value || "").toString().trim())
      .filter(Boolean);
    return candidates.some((value) => {
      const normalized = value.replace(/\*/g, " ");
      return /apl\s*pay/i.test(normalized);
    });
  };

  const matchesApplePayFilter = (row) => {
    if (!state.cardRecentAppleOnly) return true;
    return isApplePayCharge(row);
  };

  const applyCardFilters = (rows) => {
    return rows.filter((row) => matchesMemberFilter(row) && matchesAccountFilter(row));
  };

  const COPY = {
    kpi: {
      cashAvailable: "Cash available",
      cardBalancesDue: "Card balances due",
      netAfterBills: "Net after bills",
      nextBillDue: "Next bill due",
      cardBalancesDueTooltip: "Sum of Interest-free balances (fallback to statement balance) with due dates in the selected range.",
    },
    coverage: {
      title: "Bill coverage",
      covered: "Bills are covered by available cash.",
      tight: "Bills are covered, but the cushion is small.",
      shortfall: "Bills exceed available cash in this range.",
      billsDueTooltip: "Includes credit card bills and monthly checking bills due in this range.",
      projectedTooltip: "Includes the next expected occurrence within this range, even if the current cycle is already paid.",
    },
    statuses: {
      overdue: "Overdue",
      due_soon: "Due soon",
      upcoming: "Upcoming",
      paid: "Paid",
      unknown: "Unknown",
    },
    autopay: {
      on: "On",
      off: "Off",
      unknown: "Unknown",
    },
    recurring: {
      empty: "No monthly bills found.",
      emptyHelper: "Review suggestions to add monthly bills.",
      loading: "Loading monthly bills…",
      error: "Couldn’t load monthly bills.",
      suggestedEmpty: "No suggestions found.",
      suggestedHelper: "Check back after more transactions are imported.",
      activeEmpty: "No monthly bills yet.",
      recentEmpty: "No recent charges found.",
      recentHelper: "Only checking debits from the last 30 days are shown.",
    },
    cardRecurring: {
      empty: "No monthly card charges found.",
      emptyHelper: "Review suggestions to add recurring card charges.",
      loading: "Loading card charges…",
      error: "Couldn’t load card charges.",
      suggestedEmpty: "No suggestions found.",
      suggestedHelper: "Check back after more transactions are imported.",
      activeEmpty: "No recurring card charges yet.",
      recentEmpty: "No recent charges found.",
      recentHelper: "Only credit card charges from the last 30 days are shown.",
    },
    finance: {
      loading: "Loading finance charges…",
      empty: "No finance charges found.",
      error: "Couldn’t load finance charges.",
      detailLoading: "Loading charge details…",
      detailEmpty: "No finance charges found for this month.",
      detailError: "Couldn’t load charge details.",
    },
    deposits: {
      loading: "Loading deposits…",
      empty: "No deposits found for this account.",
      error: "Couldn’t load deposits.",
      detailLoading: "Loading deposits…",
      detailEmpty: "No deposits found for this month.",
      detailError: "Couldn’t load deposit details.",
    },
    empty: {
      bills: "No card bills found for this range.",
      billsHelper: "Try a longer range or check your connected accounts.",
      accounts: "No checking accounts found.",
      accountsHelper: "Connect an account or adjust your scope.",
    },
    loading: {
      bills: "Loading bills…",
      accounts: "Loading accounts…",
    },
    error: {
      bills: "Couldn’t load bills.",
      accounts: "Couldn’t load accounts.",
      retry: "Retry",
    },
  };

  const clampDate = (iso) => {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return new Date();
    return d;
  };

  const parseDate = (iso) => {
    if (!iso) return null;
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const daysInMonth = (year, monthIndex) => new Date(year, monthIndex + 1, 0).getDate();

  const dueDateForMonth = (year, monthIndex, day) => {
    const safeDay = Math.min(day, daysInMonth(year, monthIndex));
    return new Date(year, monthIndex, safeDay);
  };

  const dayOfMonthFromIso = (iso) => {
    const d = parseDate(iso);
    return d ? d.getDate() : null;
  };

  const toIsoDate = (d) => d.toISOString().slice(0, 10);

  const addDays = (d, days) => {
    const copy = new Date(d.getTime());
    copy.setDate(copy.getDate() + days);
    return copy;
  };

  const daysBetween = (a, b) => {
    const ms = b.getTime() - a.getTime();
    return Math.round(ms / (1000 * 60 * 60 * 24));
  };

  const formatDate = (iso) => {
    const d = clampDate(iso);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };

  const formatMonthLabel = (year, month) => {
    const d = new Date(Number(year), Number(month || 1) - 1, 1);
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  };

  const formatMonthLabelLong = (year, month) => {
    const d = new Date(Number(year), Number(month || 1) - 1, 1);
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString("en-US", { month: "long", year: "numeric" });
  };

  const formatDateFromParts = (year, monthIndex, day) => {
    const d = new Date(Number(year), Number(monthIndex), Number(day));
    if (Number.isNaN(d.getTime())) return "—";
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };

  const relativeTime = (iso) => {
    if (!iso) return "—";
    const d = clampDate(iso);
    const now = new Date();
    const diff = now - d;
    const minutes = Math.round(diff / 60000);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.round(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.round(hours / 24);
    if (days < 7) return `${days}d ago`;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };

  const scopeLabelText = (scope) => {
    const v = (scope || "").toString().toLowerCase();
    if (v === "personal") return "Personal";
    if (v === "family") return "Family";
    if (v === "business") return "Business";
    return "All accounts";
  };

  const applyScope = (rows, scope) => {
    if (!Array.isArray(rows)) return [];
    const v = (scope || "").toString().toLowerCase();
    if (v === "all") return rows;
    return rows.filter((r) => (r.scope || "PERSONAL").toString().toLowerCase() === v);
  };

  const normalize = (value) => (value || "").toString().toUpperCase().replace(/\s+/g, " ").trim();

  const issuerKey = (issuer) => {
    const v = normalize(issuer);
    if (v.includes("CHASE")) return "CHASE";
    if (v.includes("AMEX")) return "AMEX";
    if (v.includes("GOLDMAN") || v.includes("GS")) return "GS";
    return v || "UNKNOWN";
  };

  const mergeCardBalances = (seed) => {
    if (!balanceHints.length) return seed;
    const byLast4 = {};
    balanceHints.forEach((b) => {
      const last4 = (b.last4 || "").toString().trim();
      if (!last4) return;
      const key = `${issuerKey(b.institution)}:${last4}`;
      byLast4[key] = byLast4[key] || [];
      byLast4[key].push(b);
    });
    const updatedBills = (seed.bills || []).map((b) => {
      let current = b.current_balance;
      if (current == null) {
        const last4 = (b.last4 || "").toString().trim();
        if (last4) {
          const key = `${issuerKey(b.issuer)}:${last4}`;
          const candidates = byLast4[key];
          if (candidates && candidates.length) {
            if (candidates.length === 1) {
              current = candidates[0].balance_current;
            } else {
              const name = normalize(b.card_name);
              const matched = candidates.find((c) => normalize(c.name).includes(name) || name.includes(normalize(c.name)));
              current = (matched || candidates[0]).balance_current;
            }
          }
        }
      }
      return { ...b, current_balance: current != null ? current : b.current_balance };
    });
    return { ...seed, bills: updatedBills };
  };

  const isStatementPaid = (bill) => {
    const stmt = Number(bill.statement_balance || 0);
    const paidAmt = Number(bill.last_payment_amount || 0);
    if (!bill.last_payment_date || paidAmt <= 0) return false;
    const minDue = Number(bill.minimum_due || 0);
    const required = minDue > 0 ? minDue : stmt;
    if (required <= 0) return true;
    if (paidAmt < required * 0.98) return false;
    if (bill.last_statement_issue_date) {
      const paidDate = clampDate(bill.last_payment_date);
      const stmtDate = clampDate(bill.last_statement_issue_date);
      if (paidDate < stmtDate) return false;
    }
    return true;
  };

  const deriveStatus = (bill, asOfDate) => {
    if (!bill.due_date) return "unknown";
    if (bill.status === "paid" || isStatementPaid(bill)) return "paid";
    const due = clampDate(bill.due_date);
    if (bill.last_payment_date) {
      const paid = clampDate(bill.last_payment_date);
      if (paid >= due) return "paid";
    }
    const days = daysBetween(asOfDate, due);
    if (days < 0) return "overdue";
    return "due_soon";
  };

  const normalizeStatus = (status) => {
    const value = (status || "unknown").toLowerCase();
    return value === "upcoming" ? "due_soon" : value;
  };

  const matchesStatusFilter = (status, statusFilter) => {
    if (statusFilter === "all") return true;
    if (statusFilter === "upcoming") {
      return status === "overdue" || status === "due_soon" || status === "upcoming";
    }
    return status === statusFilter;
  };

  const getRecurringStatus = (bill) => {
    let status = normalizeStatus(bill.status || "unknown");
    if (status === "unknown" && !bill.due_date && bill.last_payment_date) {
      status = "paid";
    }
    return status;
  };

  const withinRange = (days, range, status) => {
    if (status === "overdue") return days < 0 && Math.abs(days) <= range;
    if (status === "due_soon" || status === "upcoming") return days >= 0 && days <= range;
    return Math.abs(days) <= range;
  };

  const filterBills = (bills, asOfDate, rangeDays, statusFilter) => {
    return bills
      .map((b) => ({ ...b, __status: b.due_date ? deriveStatus(b, asOfDate) : "unknown" }))
      .filter((b) => {
        const status = normalizeStatus(b.__status);
        if (!b.due_date) {
          return statusFilter === "all";
        }
        const due = clampDate(b.due_date);
        const days = daysBetween(asOfDate, due);
        if (!matchesStatusFilter(status, statusFilter)) return false;
        if (statusFilter === "upcoming" && status === "overdue") return true;
        return withinRange(days, rangeDays, status);
      })
      .sort((a, b) => {
        if (!a.due_date && !b.due_date) return 0;
        if (!a.due_date) return 1;
        if (!b.due_date) return -1;
        return a.due_date > b.due_date ? 1 : -1;
      });
  };

  const formatAmountRange = (minVal, maxVal) => {
    if (minVal != null && maxVal != null) {
      return `${currency.format(minVal)}–${currency.format(maxVal)}`;
    }
    if (minVal != null) {
      return `≥${currency.format(minVal)}`;
    }
    if (maxVal != null) {
      return `≤${currency.format(maxVal)}`;
    }
    return "—";
  };

  const formatSuggestionAmount = (item) => {
    const mode = (item.amount_mode || "").toUpperCase();
    if (mode === "FIXED") return currency.format(item.amount_expected || 0);
    if (mode === "RANGE") return formatAmountRange(item.amount_min, item.amount_max);
    return "Varies";
  };

  const formatExpectedDisplay = (bill) => {
    const raw = (bill.expected_display || "").toString().trim();
    if (!raw) return "—";
    if (raw.toLowerCase() === "varies") return "Varies";
    if (raw.includes("-")) {
      const parts = raw.split("-").map((p) => Number(p));
      if (parts.length === 2 && !Number.isNaN(parts[0]) && !Number.isNaN(parts[1])) {
        return `${currency.format(parts[0])}–${currency.format(parts[1])}`;
      }
    }
    if (raw.startsWith("≤")) {
      const val = Number(raw.slice(1));
      if (!Number.isNaN(val)) return `≤${currency.format(val)}`;
    }
    if (raw.startsWith("≥")) {
      const val = Number(raw.slice(1));
      if (!Number.isNaN(val)) return `≥${currency.format(val)}`;
    }
    const val = Number(raw);
    if (!Number.isNaN(val)) return currency.format(val);
    return raw;
  };

  const expectedAmount = (bill) => {
    const mode = (bill.amount_mode || "").toUpperCase();
    if (mode === "FIXED" && bill.amount_expected != null) {
      return Number(bill.amount_expected) || 0;
    }
    if (mode === "RANGE") {
      if (bill.amount_max != null) return Number(bill.amount_max) || 0;
      if (bill.amount_min != null) return Number(bill.amount_min) || 0;
    }
    if (bill.last_payment_amount != null) {
      return Number(bill.last_payment_amount) || 0;
    }
    return 0;
  };

  const getRecurringDueDate = (bill) => bill.due_date || bill.last_payment_date || null;

  const filterRecurringBills = (bills, asOfDate, rangeDays, statusFilter) => {
    const priority = { overdue: 0, due_soon: 1, upcoming: 2, paid: 3, unknown: 4 };
    return bills
      .filter((b) => {
        const status = getRecurringStatus(b);
        if (!matchesStatusFilter(status, statusFilter)) return false;
        const dueSource = getRecurringDueDate(b);
        if (!dueSource) return statusFilter === "unknown" || statusFilter === "all";
        const due = clampDate(dueSource);
        const days = daysBetween(asOfDate, due);
        if (statusFilter === "upcoming" && status === "overdue") return true;
        return withinRange(days, rangeDays, status);
      })
      .sort((a, b) => {
        const aStatus = getRecurringStatus(a);
        const bStatus = getRecurringStatus(b);
        const priDiff = (priority[aStatus] ?? 5) - (priority[bStatus] ?? 5);
        if (priDiff !== 0) return priDiff;
        const aDue = getRecurringDueDate(a);
        const bDue = getRecurringDueDate(b);
        if (!aDue && !bDue) return (a.name || "").localeCompare(b.name || "");
        if (!aDue) return 1;
        if (!bDue) return -1;
        if (aDue === bDue) return (a.name || "").localeCompare(b.name || "");
        return aDue > bDue ? 1 : -1;
      });
  };

  const billLiquidityAmount = (bill) => {
    if (bill.interest_saving_balance != null && Number(bill.interest_saving_balance) > 0) {
      return Number(bill.interest_saving_balance) || 0;
    }
    return Number(bill.statement_balance || 0);
  };

  const computeSummary = (bills, cash, asOfDate, rangeDays, recurringDueTotal) => {
    let cashTotal = 0;
    cash.forEach((c) => {
      const val = c.available_balance != null ? c.available_balance : c.current_balance || 0;
      cashTotal += Number(val || 0);
    });
    let cardDueTotal = 0;
    let nextDueDate = null;
    let nextDueAmount = null;
    let nextDueCard = null;
    bills.forEach((b) => {
      if (!b.due_date) return;
      const status = deriveStatus(b, asOfDate);
      if (status === "paid") return;
      const due = clampDate(b.due_date);
      const days = daysBetween(asOfDate, due);
      if (Math.abs(days) > rangeDays) return;
      cardDueTotal += billLiquidityAmount(b);
      const lastPaid = b.last_payment_date ? clampDate(b.last_payment_date) >= due : false;
      if (days < 0 || lastPaid) return;
      if (!nextDueDate || due < nextDueDate) {
        nextDueDate = due;
        nextDueAmount = billLiquidityAmount(b);
        nextDueCard = b.card_name;
      }
    });
    const dueTotal = cardDueTotal + Number(recurringDueTotal || 0);
    return {
      cashTotal,
      checkingCount: cash.length,
      cardDueTotal,
      recurringDueTotal: Number(recurringDueTotal || 0),
      dueTotal,
      netAfter: cashTotal - dueTotal,
      nextDueDate,
      nextDueAmount,
      nextDueCard,
    };
  };

  const coverageStatus = (cashTotal, dueTotal) => {
    if (dueTotal <= 0) return "covered";
    const ratio = cashTotal / dueTotal;
    if (ratio < 1) return "shortfall";
    if (ratio <= 1.1) return "tight";
    return "covered";
  };

  const expectedChargeAmount = (charge) => {
    const mode = (charge.amount_mode || "").toUpperCase();
    if (mode === "FIXED" && charge.amount_expected != null) {
      return Number(charge.amount_expected) || 0;
    }
    if (mode === "RANGE") {
      if (charge.amount_max != null) return Number(charge.amount_max) || 0;
      if (charge.amount_min != null) return Number(charge.amount_min) || 0;
    }
    if (charge.last_charge_amount != null) {
      return Number(charge.last_charge_amount) || 0;
    }
    return 0;
  };

  const computeCardRecurringDueTotal = (charges, asOfDate, rangeDays) => {
    let total = 0;
    charges.forEach((c) => {
      const status = (c.status || "unknown").toLowerCase();
      if (status === "paid") return;
      if (!c.due_date) return;
      const due = clampDate(c.due_date);
      const days = daysBetween(asOfDate, due);
      if (Math.abs(days) > rangeDays) return;
      total += expectedChargeAmount(c);
    });
    return total;
  };

  const computeCardRecurringTotalInRange = (charges, asOfDate, rangeDays) => {
    let total = 0;
    charges.forEach((c) => {
      const dueSource = c.due_date || c.last_charge_date;
      if (!dueSource) return;
      const due = clampDate(dueSource);
      const days = daysBetween(asOfDate, due);
      if (!withinRange(days, rangeDays)) return;
      total += expectedChargeAmount(c);
    });
    return total;
  };

  const computeRecurringTotalInRange = (bills, asOfDate, rangeDays) => {
    let total = 0;
    bills.forEach((b) => {
      const dueSource = getRecurringDueDate(b);
      if (!dueSource) return;
      const due = clampDate(dueSource);
      const days = daysBetween(asOfDate, due);
      if (!withinRange(days, rangeDays)) return;
      total += expectedAmount(b);
    });
    return total;
  };

  const collectCalendarDayItems = (bills, charges, cardBills, asOfDate, year, monthIndex, day) => {
    const dayNum = Number(day);
    if (!dayNum || Number.isNaN(dayNum)) return [];
    const isCurrentMonth = year === asOfDate.getFullYear() && monthIndex === asOfDate.getMonth();
    const items = [];

    const addItem = (type, label, subLabel, amount) => {
      const val = Number(amount) || 0;
      if (val <= 0) return;
      items.push({
        type,
        label: (label || "—").toString(),
        subLabel: (subLabel || "").toString(),
        amount: val,
      });
    };

    const dueDateFromDay = (dueDay) => (dueDay ? dueDateForMonth(year, monthIndex, dueDay) : null);

    bills.forEach((b) => {
      const status = getRecurringStatus(b);
      if (isCurrentMonth && status === "paid") return;
      const dueDayRaw = b.due_day_of_month != null ? Number(b.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(getRecurringDueDate(b));
      const dueDate = dueDateFromDay(dueDay);
      if (!dueDate || dueDate.getDate() !== dayNum) return;
      const label = (b.name || "").trim() || "Bill";
      const accountLabel = accountLabelFromRow(b) || b.source_account_name || "Checking";
      addItem("Monthly bill", label, accountLabel, expectedAmount(b));
    });

    charges.forEach((c) => {
      const status = normalizeStatus(c.status || "unknown");
      if (isCurrentMonth && status === "paid") return;
      const dueDayRaw = c.due_day_of_month != null ? Number(c.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(c.due_date || c.last_charge_date);
      const dueDate = dueDateFromDay(dueDay);
      if (!dueDate || dueDate.getDate() !== dayNum) return;
      const label = formatChargeLabel(c.name, c.merchant_display, c.description_sample);
      const accountLabel = accountLabelFromRow(c) || c.source_account_name || "Card";
      addItem("Card charge", label, accountLabel, expectedChargeAmount(c));
    });

    cardBills.forEach((b) => {
      if (!b || !b.due_date) return;
      if (isCurrentMonth) {
        const status = deriveStatus(b, asOfDate);
        if (status === "paid") return;
      }
      const due = parseDate(b.due_date);
      if (!due) return;
      if (due.getFullYear() !== year || due.getMonth() !== monthIndex || due.getDate() !== dayNum) return;
      addItem("Card bill", formatCardBillLabel(b), "", billLiquidityAmount(b));
    });

    return items;
  };

  const buildMonthlyCalendar = (bills, charges, cardBills, asOfDate, monthOffset = 0) => {
    const offset = Number(monthOffset) || 0;
    const monthBase = new Date(asOfDate.getFullYear(), asOfDate.getMonth() + offset, 1);
    const year = monthBase.getFullYear();
    const monthIndex = monthBase.getMonth();
    const isCurrentMonth = year === asOfDate.getFullYear() && monthIndex === asOfDate.getMonth();
    const daysInMonth = new Date(year, monthIndex + 1, 0).getDate();
    const firstDow = new Date(year, monthIndex, 1).getDay();
    const totals = Array.from({ length: daysInMonth }, () => 0);

    const addAmount = (dueDate, amount) => {
      if (!dueDate || amount == null) return;
      const due = clampDate(dueDate);
      if (due.getFullYear() !== year || due.getMonth() !== monthIndex) return;
      const val = Number(amount) || 0;
      if (val <= 0) return;
      totals[due.getDate() - 1] += val;
    };

    const dueDateFromDay = (day) => (day ? dueDateForMonth(year, monthIndex, day) : null);

    bills.forEach((b) => {
      const status = getRecurringStatus(b);
      if (isCurrentMonth && status === "paid") return;
      const dueDayRaw = b.due_day_of_month != null ? Number(b.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(getRecurringDueDate(b));
      const dueDate = dueDateFromDay(dueDay);
      if (!dueDate) return;
      addAmount(dueDate, expectedAmount(b));
    });

    charges.forEach((c) => {
      const status = normalizeStatus(c.status || "unknown");
      if (isCurrentMonth && status === "paid") return;
      const dueDayRaw = c.due_day_of_month != null ? Number(c.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(c.due_date || c.last_charge_date);
      const dueDate = dueDateFromDay(dueDay);
      if (!dueDate) return;
      addAmount(dueDate, expectedChargeAmount(c));
    });

    cardBills.forEach((b) => {
      if (!b || !b.due_date) return;
      if (isCurrentMonth) {
        const status = deriveStatus(b, asOfDate);
        if (status === "paid") return;
      }
      addAmount(b.due_date, billLiquidityAmount(b));
    });

    const dowLabels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const header = dowLabels.map((d) => `<div class="cashbills-calendar__dow">${d}</div>`).join("");

    const cells = [];
    for (let i = 0; i < firstDow; i += 1) {
      cells.push(`<div class="cashbills-calendar__cell cashbills-calendar__cell--empty"></div>`);
    }
    for (let day = 1; day <= daysInMonth; day += 1) {
      const amt = totals[day - 1] || 0;
      const amountLabel = amt > 0 ? currency.format(amt) : "-";
      const dueClass = amt > 0 ? " cashbills-calendar__cell--due" : "";
      const detailsLink =
        amt > 0
          ? `<button class="btn btn--link btn--sm cashbills-calendar__link" type="button" data-calendar-year="${year}" data-calendar-month="${monthIndex + 1}" data-calendar-day="${day}">Items</button>`
          : "";
      cells.push(`
        <div class="cashbills-calendar__cell${dueClass}">
          <div class="cashbills-calendar__day">${day}</div>
          <div class="cashbills-calendar__details">
            <div class="cashbills-calendar__amount ui-tabular-nums">${amountLabel}</div>
            ${detailsLink}
          </div>
        </div>
      `);
    }
    while (cells.length % 7 !== 0) {
      cells.push(`<div class="cashbills-calendar__cell cashbills-calendar__cell--empty"></div>`);
    }

    const monthLabel = formatMonthLabelLong(year, monthIndex + 1);
    return `
      <div class="cashbills-monthly-divider"></div>
      <div class="cashbills-monthly-header cashbills-calendar__header">
        <div class="cashbills-calendar__heading">
          <div class="ui-muted">Monthly calendar</div>
          <div class="cashbills-calendar__title">${monthLabel}</div>
        </div>
        <div class="cashbills-calendar__nav">
          <button class="btn btn--secondary btn--sm" type="button" id="cashBillsCalendarPrev" aria-label="Previous month">Prev</button>
          <button class="btn btn--secondary btn--sm" type="button" id="cashBillsCalendarNext" aria-label="Next month">Next</button>
        </div>
      </div>
      <div class="cashbills-calendar">
        <div class="cashbills-calendar__grid">
          ${header}
          ${cells.join("")}
        </div>
      </div>
      <div class="ui-muted" style="margin-top:6px">Amounts shown are due in this month.</div>
    `;
  };

  const typicalCardBillAmount = (bill) => {
    if (bill.interest_saving_balance != null && Number(bill.interest_saving_balance) > 0) {
      return Number(bill.interest_saving_balance) || 0;
    }
    if (bill.last_payment_amount != null && Number(bill.last_payment_amount) > 0) {
      return Number(bill.last_payment_amount) || 0;
    }
    if (bill.statement_balance != null) {
      return Number(bill.statement_balance) || 0;
    }
    if (bill.minimum_due != null) {
      return Number(bill.minimum_due) || 0;
    }
    return 0;
  };

  const projectOccurrences = (dueDay, asOfDate, rangeDays, startOffset = 0) => {
    const occurrences = [];
    const endDate = addDays(asOfDate, rangeDays);
    const maxOffset = startOffset + 2;
    for (let offset = startOffset; offset <= maxOffset; offset += 1) {
      const monthBase = new Date(asOfDate.getFullYear(), asOfDate.getMonth() + offset, 1);
      const dueDate = dueDateForMonth(monthBase.getFullYear(), monthBase.getMonth(), dueDay);
      if (dueDate < asOfDate || dueDate > endDate) continue;
      occurrences.push(dueDate);
    }
    return occurrences;
  };

  const projectCardBillsTotal = (bills, asOfDate, rangeDays) => {
    let total = 0;
    bills.forEach((b) => {
      const dueDay = dayOfMonthFromIso(b.due_date) || dayOfMonthFromIso(b.last_payment_date);
      if (!dueDay) return;
      const status = deriveStatus(b, asOfDate);
      const startOffset = status === "paid" ? 1 : 0;
      const occurrences = projectOccurrences(dueDay, asOfDate, rangeDays, startOffset);
      if (!occurrences.length) return;
      total += typicalCardBillAmount(b) * occurrences.length;
    });
    return total;
  };

  const projectRecurringBillsTotal = (bills, asOfDate, rangeDays) => {
    let total = 0;
    bills.forEach((b) => {
      const dueDayRaw = b.due_day_of_month != null ? Number(b.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(getRecurringDueDate(b));
      if (!dueDay) return;
      const status = getRecurringStatus(b);
      const startOffset = status === "paid" ? 1 : 0;
      const occurrences = projectOccurrences(dueDay, asOfDate, rangeDays, startOffset);
      if (!occurrences.length) return;
      total += expectedAmount(b) * occurrences.length;
    });
    return total;
  };

  const projectCardRecurringChargesTotal = (charges, asOfDate, rangeDays) => {
    let total = 0;
    charges.forEach((c) => {
      const dueDayRaw = c.due_day_of_month != null ? Number(c.due_day_of_month) : null;
      const dueDay = dueDayRaw || dayOfMonthFromIso(c.due_date || c.last_charge_date);
      if (!dueDay) return;
      const status = normalizeStatus(c.status || "unknown");
      const startOffset = status === "paid" ? 1 : 0;
      const occurrences = projectOccurrences(dueDay, asOfDate, rangeDays, startOffset);
      if (!occurrences.length) return;
      total += expectedChargeAmount(c) * occurrences.length;
    });
    return total;
  };

  const computeProjectedTotals = (bills, recurringBills, cardRecurringCharges, asOfDate, rangeDays, ready) => {
    if (!ready) {
      return {
        ready: false,
        cardBills: 0,
        monthlyBills: 0,
        monthlyCardCharges: 0,
        total: 0,
      };
    }
    const projectedCardBills = projectCardBillsTotal(bills, asOfDate, rangeDays);
    const projectedBills = projectRecurringBillsTotal(recurringBills, asOfDate, rangeDays);
    const projectedCharges = projectCardRecurringChargesTotal(cardRecurringCharges, asOfDate, rangeDays);
    const total = projectedCardBills + projectedBills + projectedCharges;
    return {
      ready: true,
      cardBills: projectedCardBills,
      monthlyBills: projectedBills,
      monthlyCardCharges: projectedCharges,
      total,
    };
  };

  const renderKpis = (summary, rangeDays) => {
    if (!els.kpis) return;
    const rangeLabel = `${rangeDays} days`;
    const cardLabel = (summary.nextDueCard || "").trim();
    const cardPlaceholder = ["CREDIT CARD"].includes(cardLabel.toUpperCase());
    const nextDueSubtext = summary.nextDueDate
      ? `${cardLabel && !cardPlaceholder ? cardLabel + " • " : "Card • "}Due ${formatDate(summary.nextDueDate)}`
      : "Next due date unavailable";
    els.kpis.innerHTML = `
      <div class="ui-card ui-kpi ui-tone-neutral">
        <div class="ui-card__label">${COPY.kpi.cashAvailable}</div>
        <div class="ui-card__value ui-tabular-nums">${currency.format(summary.cashTotal)}</div>
        <div class="ui-card__subtext ui-muted">Across ${summary.checkingCount} checking accounts</div>
      </div>
      <div class="ui-card ui-kpi ui-tone-neutral">
        <div class="ui-card__label" title="${COPY.kpi.cardBalancesDueTooltip}">${COPY.kpi.cardBalancesDue}</div>
        <div class="ui-card__value ui-tabular-nums">${currency.format(summary.cardDueTotal)}</div>
        <div class="ui-card__subtext ui-muted">Credit cards only · Due in the next ${rangeLabel}</div>
      </div>
      <div class="ui-card ui-kpi ${summary.netAfter < 0 ? "ui-tone-warning" : "ui-tone-neutral"}">
        <div class="ui-card__label">${COPY.kpi.netAfterBills}</div>
        <div class="ui-card__value ui-tabular-nums">${currency.format(summary.netAfter)}</div>
        <div class="ui-card__subtext ui-muted">Cash minus bills due</div>
      </div>
      <div class="ui-card ui-kpi ui-tone-neutral">
        <div class="ui-card__label">${COPY.kpi.nextBillDue}</div>
        <div class="ui-card__value ui-tabular-nums">${summary.nextDueAmount != null ? currency.format(summary.nextDueAmount) : "—"}</div>
        <div class="ui-card__subtext ui-muted">${nextDueSubtext}</div>
      </div>
    `;
  };

  const renderBills = (rows, asOfDate) => {
    if (!els.billsTable) return;
    if (state.billsError) {
      els.billsTable.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.error.bills}</div>
          <div class="ui-muted" style="margin-top:4px">${state.billsError}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryBills">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryBills");
      if (retry) retry.addEventListener("click", () => loadData());
      return;
    }
    if (!rows.length) {
      els.billsTable.innerHTML = `
        <div class="ui-muted">${COPY.empty.bills}</div>
        <div class="ui-muted" style="margin-top:4px">${COPY.empty.billsHelper}</div>
      `;
      return;
    }
    const warningHtml = state.billsWarning
      ? `<div class="alert alert--warn" style="margin-bottom:12px"><div>${state.billsWarning}</div></div>`
      : "";
    const tableRows = rows
      .map((b) => {
        const hasDue = Boolean(b.due_date);
        const due = hasDue ? clampDate(b.due_date) : null;
        const days = due ? daysBetween(asOfDate, due) : null;
        const status = normalizeStatus(hasDue ? (b.__status || deriveStatus(b, asOfDate)) : "unknown");
        const statusLabel = COPY.statuses[status] || status.replace("_", " ");
        const overdueClass = status === "overdue" ? " cashbills-badge--overdue" : "";
        const statusClass =
          (status === "overdue" ? "ui-badge--bad" : status === "due_soon" ? "ui-badge--risk" : status === "paid" ? "ui-badge--safe" : "ui-badge--neutral") +
          overdueClass;
        let daysLabel = "—";
        let daysClass = "ui-badge--outline";
        if (days != null) {
          daysLabel = days === 0 ? "Due today" : days < 0 ? `${Math.abs(days)}d overdue` : `${days}d left`;
          daysClass = days < 0 ? "ui-badge--bad cashbills-badge--overdue" : days <= 7 ? "ui-badge--risk" : "ui-badge--outline";
        }
        const dueMeta =
          status === "paid"
            ? `<span class="ui-badge ui-badge--safe">Paid</span>`
            : `<span class="ui-badge ${statusClass}">${statusLabel}</span><span class="ui-badge ${daysClass}">${daysLabel}</span>`;
        const cardLabel = `${b.issuer ? b.issuer + " " : ""}${b.card_name}${b.last4 ? " • " + b.last4 : ""}`;
        const lastPaymentDate = b.last_payment_date ? formatDate(b.last_payment_date) : "—";
        const lastPaymentAmt = b.last_payment_amount != null ? currency.format(b.last_payment_amount) : "—";
        const interestSaving = b.interest_saving_balance != null ? currency.format(b.interest_saving_balance) : "—";
        const payOverRows = b.pay_over_time && Array.isArray(b.pay_over_time.rows) ? b.pay_over_time.rows : [];
        const payOverBtn =
          payOverRows.length > 0
            ? `<button class="btn btn--link cashbills-payover-btn js-payover" type="button" data-bill-id="${b.id}">Pay Over Time details</button>`
            : "";
        const interestCell = `
          <div class="cashbills-interest-cell">
            <div class="ui-tabular-nums">${interestSaving}</div>
            ${payOverBtn}
          </div>
        `;
        return `
          <tr>
            <td>
              <div>${b.due_date ? formatDate(b.due_date) : "—"}</div>
              <div class="cashbills-due-meta">${dueMeta}</div>
            </td>
            <td>
              <div class="cashbills-card-name card-cell">${cardLabel}</div>
              <div class="ui-muted">${b.card_name}</div>
            </td>
            <td class="num ui-tabular-nums">${currency.format(b.statement_balance || 0)}</td>
            <td class="num">${interestCell}</td>
            <td class="num ui-tabular-nums">${b.minimum_due != null ? currency.format(b.minimum_due) : "—"}</td>
            <td class="num ui-tabular-nums">${b.current_balance != null ? currency.format(b.current_balance) : "—"}</td>
            <td>
              <div>${lastPaymentDate}</div>
              <div class="ui-muted ui-tabular-nums">${lastPaymentAmt}</div>
            </td>
          </tr>
        `;
      })
      .join("");

    els.billsTable.innerHTML = `
      ${warningHtml}
      <div class="table-wrapper">
        <table class="data-table bills-table">
          <colgroup>
            <col style="width:130px" />
            <col />
            <col style="width:170px" />
            <col style="width:190px" />
            <col style="width:140px" />
            <col style="width:160px" />
            <col style="width:170px" />
          </colgroup>
          <thead>
            <tr>
              <th>Due date</th>
              <th>Card</th>
              <th class="num">Statement balance</th>
              <th class="num">Interest-free balance</th>
              <th class="num">Minimum due</th>
              <th class="num">Current balance</th>
              <th>Last payment</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderRecurring = (rows, asOfDate) => {
    if (!els.recurringTable) return;
    if (state.recurringLoading) {
      els.recurringTable.innerHTML = `<div class="ui-muted">${COPY.recurring.loading}</div>`;
      return;
    }
    if (state.recurringError) {
      els.recurringTable.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.recurring.error}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryRecurring">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryRecurring");
      if (retry) retry.addEventListener("click", () => refreshRecurringSummary());
      return;
    }
    if (!rows.length) {
      els.recurringTable.innerHTML = `
        <div class="ui-muted">${COPY.recurring.empty}</div>
        <div class="ui-muted" style="margin-top:4px">${COPY.recurring.emptyHelper}</div>
      `;
      return;
    }
    const tableRows = rows
      .map((b) => {
        const status = getRecurringStatus(b);
        const statusLabel = COPY.statuses[status] || status;
        const overdueClass = status === "overdue" ? " cashbills-badge--overdue" : "";
        const statusClass =
          (status === "overdue" ? "ui-badge--bad" : status === "due_soon" ? "ui-badge--risk" : status === "paid" ? "ui-badge--safe" : "ui-badge--neutral") +
          overdueClass;
        const dueSource = getRecurringDueDate(b);
        let dueCell = `<div>—</div><button class="btn btn--link js-set-due" data-bill-id="${b.id}">Set due date</button>`;
        if (dueSource) {
          const due = clampDate(dueSource);
          const days = daysBetween(asOfDate, due);
          let daysLabel = days === 0 ? "Due today" : days < 0 ? `${Math.abs(days)}d overdue` : `${days}d left`;
          let daysClass = days < 0 ? "ui-badge--bad cashbills-badge--overdue" : days <= 7 ? "ui-badge--risk" : "ui-badge--outline";
          const dueMeta =
            status === "paid"
              ? `<span class="ui-badge ui-badge--safe">Paid</span>`
              : `<span class="ui-badge ${statusClass}">${statusLabel}</span><span class="ui-badge ${daysClass}">${daysLabel}</span>`;
          dueCell = `<div>${formatDate(dueSource)}</div><div class="cashbills-due-meta">${dueMeta}</div>`;
        }
        const expected = formatExpectedDisplay(b);
        const expectedTitle =
          expected === "Varies" ? "Amount changes month to month based on recent payments." : "";
        const lastPaymentDate = b.last_payment_date ? formatDate(b.last_payment_date) : "—";
        const lastPaymentAmt = b.last_payment_amount != null ? currency.format(b.last_payment_amount) : "—";
        const sourceLabel = b.source_account_name ? `Detected from ${b.source_account_name}` : "Detected from checking";
        return `
          <tr>
            <td>${dueCell}</td>
            <td>
              <div class="cashbills-card-name">${b.name}</div>
              <div class="ui-muted">${sourceLabel}</div>
            </td>
            <td class="num ui-tabular-nums">
              <span${expectedTitle ? ` title="${expectedTitle}"` : ""}>${expected}</span>
            </td>
            <td>
              <div>${lastPaymentDate}</div>
              <div class="ui-muted ui-tabular-nums">${lastPaymentAmt}</div>
            </td>
          </tr>
        `;
      })
      .join("");

    els.recurringTable.innerHTML = `
      <div class="table-wrapper">
        <table class="data-table bills-table cashbills-recurring-table">
          <colgroup>
            <col style="width:130px" />
            <col />
            <col style="width:160px" />
            <col style="width:170px" />
          </colgroup>
          <thead>
            <tr>
              <th>Due date</th>
              <th>Bill</th>
              <th class="num">Expected</th>
              <th>Last payment</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderCardRecurring = (rows, asOfDate) => {
    if (!els.cardRecurringTable) return;
    if (state.cardRecurringLoading) {
      els.cardRecurringTable.innerHTML = `<div class="ui-muted">${COPY.cardRecurring.loading}</div>`;
      return;
    }
    if (state.cardRecurringError) {
      els.cardRecurringTable.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.cardRecurring.error}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryCardRecurring">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryCardRecurring");
      if (retry) retry.addEventListener("click", () => refreshCardRecurringSummary());
      return;
    }
    if (!rows.length) {
      els.cardRecurringTable.innerHTML = `
        <div class="ui-muted">${COPY.cardRecurring.empty}</div>
        <div class="ui-muted" style="margin-top:4px">${COPY.cardRecurring.emptyHelper}</div>
      `;
      return;
    }
    const tableRows = rows
      .map((c) => {
        const status = normalizeStatus(c.status || "unknown");
        const statusLabel = COPY.statuses[status] || status;
        const overdueClass = status === "overdue" ? " cashbills-badge--overdue" : "";
        const statusClass =
          (status === "overdue" ? "ui-badge--bad" : status === "due_soon" ? "ui-badge--risk" : status === "paid" ? "ui-badge--safe" : "ui-badge--neutral") +
          overdueClass;
        let dueCell = `<div>—</div><button class="btn btn--link js-set-card-due" data-charge-id="${c.id}">Set charge day</button>`;
        if (c.due_date) {
          const due = clampDate(c.due_date);
          const days = daysBetween(asOfDate, due);
          let daysLabel = days === 0 ? "Due today" : days < 0 ? `${Math.abs(days)}d overdue` : `${days}d left`;
          let daysClass = days < 0 ? "ui-badge--bad cashbills-badge--overdue" : days <= 7 ? "ui-badge--risk" : "ui-badge--outline";
          const dueMeta =
            status === "paid"
              ? `<span class="ui-badge ui-badge--safe">Paid</span>`
              : `<span class="ui-badge ${statusClass}">${statusLabel}</span><span class="ui-badge ${daysClass}">${daysLabel}</span>`;
          dueCell = `<div>${formatDate(c.due_date)}</div><div class="cashbills-due-meta">${dueMeta}</div>`;
        }
        const expected = formatExpectedDisplay(c);
        const expectedTitle =
          expected === "Varies" ? "Amount changes month to month based on recent charges." : "";
        const lastChargeDate = c.last_charge_date ? formatDate(c.last_charge_date) : "—";
        const lastChargeAmt = c.last_charge_amount != null ? currency.format(c.last_charge_amount) : "—";
        const sourceLabel = c.source_account_name ? `Detected from ${c.source_account_name}` : "Detected from card";
        return `
          <tr>
            <td>${dueCell}</td>
            <td>
              <div class="cashbills-card-name">${c.name}</div>
              <div class="ui-muted">${sourceLabel}</div>
            </td>
            <td class="num ui-tabular-nums">
              <span${expectedTitle ? ` title="${expectedTitle}"` : ""}>${expected}</span>
            </td>
            <td>
              <div>${lastChargeDate}</div>
              <div class="ui-muted ui-tabular-nums">${lastChargeAmt}</div>
            </td>
          </tr>
        `;
      })
      .join("");

    els.cardRecurringTable.innerHTML = `
      <div class="table-wrapper">
        <table class="data-table bills-table cashbills-card-recurring-table">
          <colgroup>
            <col style="width:130px" />
            <col />
            <col style="width:160px" />
            <col style="width:170px" />
          </colgroup>
          <thead>
            <tr>
              <th>Charge date</th>
              <th>Charge</th>
              <th class="num">Expected</th>
              <th>Last charge</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderFinanceCharges = (rows) => {
    if (!els.financeTable) return;
    if (state.financeLoading) {
      els.financeTable.innerHTML = `<div class="ui-muted">${COPY.finance.loading}</div>`;
      return;
    }
    if (state.financeError) {
      els.financeTable.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.finance.error}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryFinance">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryFinance");
      if (retry) retry.addEventListener("click", () => refreshFinanceCharges());
      return;
    }
    if (!rows.length) {
      els.financeTable.innerHTML = `<div class="ui-muted">${COPY.finance.empty}</div>`;
      return;
    }
    const tableRows = rows
      .map((row) => {
        const monthLabel = formatMonthLabel(row.year, row.month);
        const cardLabel = formatCardLabel(row.card_name, row.card_last4);
        const amount = currency.format(row.amount || 0);
        const count = row.count ? `${row.count} charge${row.count === 1 ? "" : "s"}` : "";
        const viewAction =
          row.count && row.count > 1
            ? `<button class="btn btn--link btn--sm cashbills-finance-view" type="button" data-finance-year="${row.year}" data-finance-month="${row.month}" data-finance-account="${row.account_id}" data-finance-card="${row.card_name || ""}" data-finance-last4="${row.card_last4 || ""}">View</button>`
            : "";
        return `
          <tr>
            <td>${monthLabel}</td>
            <td>
              <div class="cashbills-card-name">${cardLabel || "—"}</div>
              <div class="ui-muted cashbills-finance-meta">
                <span>${count || "—"}</span>
                ${viewAction}
              </div>
            </td>
            <td class="num ui-tabular-nums">${amount}</td>
          </tr>
        `;
      })
      .join("");
    els.financeTable.innerHTML = `
      <div class="table-wrapper">
        <table class="data-table bills-table cashbills-finance-table">
          <colgroup>
            <col style="width:140px" />
            <col />
            <col style="width:160px" />
          </colgroup>
          <thead>
            <tr>
              <th>Month</th>
              <th>Card</th>
              <th class="num">Finance charges</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
    const viewButtons = els.financeTable.querySelectorAll(".cashbills-finance-view");
    viewButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const year = Number(btn.getAttribute("data-finance-year"));
        const month = Number(btn.getAttribute("data-finance-month"));
        const accountId = Number(btn.getAttribute("data-finance-account"));
        if (!year || !month || !accountId) return;
        openFinanceModal({
          year,
          month,
          account_id: accountId,
          card_name: btn.getAttribute("data-finance-card") || "",
          card_last4: btn.getAttribute("data-finance-last4") || "",
        });
      });
    });
  };

  const renderFinanceSection = () => {
    renderFinanceCharges(state.financeRows || []);
    if (els.financeSummary) {
      if (state.financeLoading || state.financeError) {
        els.financeSummary.textContent = "—";
        return;
      }
      const total = (state.financeRows || []).reduce((sum, row) => sum + Number(row.amount || 0), 0);
      const months = state.financeMonths || 12;
      els.financeSummary.textContent = `${months} months · ${currency.format(total)} total`;
    }
  };

  const renderMonthlyTotals = () => {
    if (!els.monthlyTotals) return;
    const totalsReady = !(state.recurringLoading || state.cardRecurringLoading || state.recurringError || state.cardRecurringError);
    const asOfDate = clampDate(state.asOfDate);
    const rangeLabel = `${state.rangeDays} days`;
    const totalsHtml = totalsReady
      ? (() => {
          const cardTotal = computeCardRecurringTotalInRange(state.cardRecurringCharges || [], asOfDate, state.rangeDays);
          const checkingTotal = computeRecurringTotalInRange(state.recurringBills || [], asOfDate, state.rangeDays);
          return `
            <div class="ui-muted">Next ${rangeLabel}</div>
            <div class="cashbills-coverage" style="margin-top:8px">
              <div class="cashbills-coverage__row">
                <span>Monthly card charges</span>
                <span class="ui-tabular-nums">${currency.format(cardTotal)}</span>
              </div>
              <div class="cashbills-coverage__row">
                <span>Monthly bills (checking)</span>
                <span class="ui-tabular-nums">${currency.format(checkingTotal)}</span>
              </div>
            </div>
            <div class="ui-muted" style="margin-top:6px">Totals include items due/charged in this range.</div>
          `;
        })()
      : `<div class="ui-muted">—</div>`;

    const scopedCardBills = state.data ? applyScope(state.data.bills || [], state.scope) : [];
    const calendarHtml = totalsReady
      ? buildMonthlyCalendar(state.recurringBills || [], state.cardRecurringCharges || [], scopedCardBills, asOfDate, state.calendarMonthOffset)
      : `<div class="ui-muted" style="margin-top:12px">Calendar loading...</div>`;

    let depositsHtml = "";
    if (state.depositsLoading) {
      depositsHtml = `<div class="ui-muted" style="margin-top:12px">${COPY.deposits.loading}</div>`;
    } else if (state.depositsError) {
      depositsHtml = `
        <div class="alert alert--warn" style="margin-top:12px">
          <div>${COPY.deposits.error}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryDeposits">${COPY.error.retry}</button>
        </div>
      `;
    } else if (!state.depositsAccounts.length) {
      depositsHtml = `<div class="ui-muted" style="margin-top:12px">No checking accounts available.</div>`;
    } else {
      const selected = state.depositsAccountId || String(state.depositsAccounts[0].id);
      const options = state.depositsAccounts
        .map((acct) => {
          const id = String(acct.id);
          const label = acct.label || acct.name || `Account ${id}`;
          return `<option value="${id}" ${id === selected ? "selected" : ""}>${label}</option>`;
        })
        .join("");
      const rows = (state.depositsMonthly || [])
        .map((row) => {
          const monthLabel = formatMonthLabel(row.year, row.month);
          return `
            <div class="cashbills-coverage__row">
              <span>${monthLabel}</span>
              <span class="cashbills-deposit-meta">
                <span class="ui-tabular-nums">${currency.format(row.amount || 0)}</span>
                <button class="btn btn--link btn--sm cashbills-deposits-view" type="button" data-deposit-year="${row.year}" data-deposit-month="${row.month}">View</button>
              </span>
            </div>
          `;
        })
        .join("");
      depositsHtml = `
        <div class="cashbills-monthly-divider"></div>
        <div class="cashbills-monthly-header">
          <div class="ui-muted">Monthly cash deposits</div>
          <select id="cashBillsDepositAccount" class="cashbills-deposit-select">
            ${options}
          </select>
        </div>
        ${rows ? `<div class="cashbills-coverage" style="margin-top:8px">${rows}</div>` : `<div class="ui-muted" style="margin-top:8px">${COPY.deposits.empty}</div>`}
        <div class="ui-muted" style="margin-top:6px">Last ${state.depositsMonths || 6} months</div>
      `;
    }

    els.monthlyTotals.innerHTML = `
      ${totalsHtml}
      ${calendarHtml}
      ${depositsHtml}
    `;
    const retryDeposits = document.getElementById("cashBillsRetryDeposits");
    if (retryDeposits) {
      retryDeposits.addEventListener("click", () => refreshDepositSummary(true));
    }
    const calendarPrev = document.getElementById("cashBillsCalendarPrev");
    if (calendarPrev) {
      calendarPrev.addEventListener("click", () => {
        state.calendarMonthOffset -= 1;
        renderMonthlyTotals();
      });
    }
    const calendarNext = document.getElementById("cashBillsCalendarNext");
    if (calendarNext) {
      calendarNext.addEventListener("click", () => {
        state.calendarMonthOffset += 1;
        renderMonthlyTotals();
      });
    }
    const calendarLinks = els.monthlyTotals.querySelectorAll(".cashbills-calendar__link");
    calendarLinks.forEach((btn) => {
      btn.addEventListener("click", () => {
        const year = Number(btn.getAttribute("data-calendar-year"));
        const month = Number(btn.getAttribute("data-calendar-month"));
        const day = Number(btn.getAttribute("data-calendar-day"));
        if (!year || !month || !day) return;
        openCalendarModal({ year, monthIndex: month - 1, day });
      });
    });
    const viewButtons = els.monthlyTotals.querySelectorAll(".cashbills-deposits-view");
    viewButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        const year = Number(btn.getAttribute("data-deposit-year"));
        const month = Number(btn.getAttribute("data-deposit-month"));
        const accountId = Number(state.depositsAccountId || "");
        if (!year || !month || !accountId) return;
        openDepositsModal({ year, month, account_id: accountId });
      });
    });
  };

  const renderFinanceModal = () => {
    if (!els.financeModal) return;
    els.financeModal.classList.toggle("is-open", state.financeModalOpen);
    els.financeModal.setAttribute("aria-hidden", state.financeModalOpen ? "false" : "true");
    if (!state.financeModalOpen || !els.financeModalContent) return;

    const context = state.financeModalContext || {};
    const monthLabel = context.year && context.month ? formatMonthLabel(context.year, context.month) : "Finance charges";
    const cardLabel = formatCardLabel(context.card_name, context.card_last4);
    if (els.financeModalTitle) {
      els.financeModalTitle.textContent = cardLabel && cardLabel !== "—" ? `${monthLabel} · ${cardLabel}` : `${monthLabel} finance charges`;
    }

    if (state.financeModalLoading) {
      els.financeModalContent.innerHTML = `<div class="ui-muted">${COPY.finance.detailLoading}</div>`;
      return;
    }
    if (state.financeModalError) {
      els.financeModalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.finance.detailError}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsFinanceRetry">Retry</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsFinanceRetry");
      if (retry) {
        retry.addEventListener("click", () => {
          if (context.year && context.month && context.account_id) {
            loadFinanceTransactions(context);
          }
        });
      }
      return;
    }
    if (!state.financeModalRows.length) {
      els.financeModalContent.innerHTML = `<div class="ui-muted">${COPY.finance.detailEmpty}</div>`;
      return;
    }
    const total = state.financeModalRows.reduce((sum, row) => sum + Number(row.amount || 0), 0);
    const tableRows = state.financeModalRows
      .map((row) => {
        const dateLabel = formatDate(row.posted_date);
        const desc = row.description || "—";
        const amount = currency.format(row.amount || 0);
        return `
          <tr>
            <td>${dateLabel}</td>
            <td>${desc}</td>
            <td class="num ui-tabular-nums">${amount}</td>
          </tr>
        `;
      })
      .join("");

    els.financeModalContent.innerHTML = `
      <div class="ui-muted" style="margin-bottom:8px">Total for month: ${currency.format(total)}</div>
      <div class="table-wrapper">
        <table class="data-table cashbills-finance-detail-table">
          <colgroup>
            <col style="width:140px" />
            <col />
            <col style="width:160px" />
          </colgroup>
          <thead>
            <tr>
              <th>Date</th>
              <th>Description</th>
              <th class="num">Amount</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderDepositsModal = () => {
    if (!els.depositsModal) return;
    els.depositsModal.classList.toggle("is-open", state.depositsModalOpen);
    els.depositsModal.setAttribute("aria-hidden", state.depositsModalOpen ? "false" : "true");
    if (!state.depositsModalOpen || !els.depositsModalContent) return;

    const context = state.depositsModalContext || {};
    const monthLabel = context.year && context.month ? formatMonthLabel(context.year, context.month) : "Deposits";
    const accountLabel = context.account_label || "Checking account";
    if (els.depositsModalTitle) {
      els.depositsModalTitle.textContent = `${monthLabel} · ${accountLabel}`;
    }

    if (state.depositsModalLoading) {
      els.depositsModalContent.innerHTML = `<div class="ui-muted">${COPY.deposits.detailLoading}</div>`;
      return;
    }
    if (state.depositsModalError) {
      els.depositsModalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.deposits.detailError}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsDepositsRetry">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsDepositsRetry");
      if (retry) {
        retry.addEventListener("click", () => loadDepositTransactions(context));
      }
      return;
    }
    if (!state.depositsModalRows.length) {
      els.depositsModalContent.innerHTML = `<div class="ui-muted">${COPY.deposits.detailEmpty}</div>`;
      return;
    }

    const total = state.depositsModalRows.reduce((sum, row) => sum + Number(row.amount || 0), 0);
    const tableRows = state.depositsModalRows
      .map((row) => {
        const dateLabel = formatDate(row.posted_date);
        const desc = (row.description || "").trim() || "Deposit";
        return `
          <tr>
            <td>${dateLabel}</td>
            <td>${desc}</td>
            <td class="num ui-tabular-nums">${currency.format(row.amount || 0)}</td>
          </tr>
        `;
      })
      .join("");
    els.depositsModalContent.innerHTML = `
      <div class="ui-muted" style="margin-bottom:8px">Total for month: ${currency.format(total)}</div>
      <div class="table-wrapper">
        <table class="data-table cashbills-deposits-detail-table">
          <colgroup>
            <col style="width:140px" />
            <col />
            <col style="width:160px" />
          </colgroup>
          <thead>
            <tr>
              <th>Date</th>
              <th>Description</th>
              <th class="num">Amount</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderPayOverModal = () => {
    if (!els.payOverModal) return;
    els.payOverModal.classList.toggle("is-open", state.payOverModalOpen);
    els.payOverModal.setAttribute("aria-hidden", state.payOverModalOpen ? "false" : "true");
    if (!state.payOverModalOpen || !els.payOverModalContent) return;

    const context = state.payOverModalContext || {};
    const cardLabel = context.card_label || "";
    const data = context.pay_over_time || {};
    const rows = Array.isArray(data.rows) ? data.rows : [];
    const totals = data.totals && typeof data.totals === "object" ? data.totals : null;
    const paymentDueTotal = data.payment_due_total;

    if (els.payOverModalTitle) {
      els.payOverModalTitle.textContent = cardLabel && cardLabel !== "—" ? `Pay Over Time plans · ${cardLabel}` : "Pay Over Time plans";
    }

    if (!rows.length) {
      els.payOverModalContent.innerHTML = `<div class="ui-muted">No Pay Over Time plans found in this statement.</div>`;
      return;
    }

    const fmtMoney = (value) => (value != null ? currency.format(value) : "—");
    const paymentDueNote =
      paymentDueTotal != null
        ? `<div class="ui-muted" style="margin-bottom:8px">Payment due for plans set up after purchase: ${fmtMoney(paymentDueTotal)}</div>`
        : "";
    const tableRows = rows
      .map((row) => {
        return `
          <tr>
            <td>${row.description || "—"}</td>
            <td>${row.plan_start_date ? formatDate(row.plan_start_date) : "—"}</td>
            <td class="num ui-tabular-nums">${fmtMoney(row.original_principal)}</td>
            <td class="num ui-tabular-nums">${row.total_payments != null ? row.total_payments : "—"}</td>
            <td class="num ui-tabular-nums">${fmtMoney(row.remaining_principal)}</td>
            <td class="num ui-tabular-nums">${row.remaining_payments != null ? row.remaining_payments : "—"}</td>
            <td class="num ui-tabular-nums">${fmtMoney(row.monthly_principal)}</td>
            <td class="num ui-tabular-nums">${fmtMoney(row.monthly_fee)}</td>
            <td class="num ui-tabular-nums">${fmtMoney(row.payment_due)}</td>
          </tr>
        `;
      })
      .join("");
    const totalsRow = totals
      ? `
        <tr class="cashbills-payover-total">
          <td>Plan totals</td>
          <td>—</td>
          <td class="num ui-tabular-nums">${fmtMoney(totals.original_principal)}</td>
          <td class="num ui-tabular-nums">—</td>
          <td class="num ui-tabular-nums">${fmtMoney(totals.remaining_principal)}</td>
          <td class="num ui-tabular-nums">—</td>
          <td class="num ui-tabular-nums">${fmtMoney(totals.monthly_principal)}</td>
          <td class="num ui-tabular-nums">${fmtMoney(totals.monthly_fee)}</td>
          <td class="num ui-tabular-nums">${fmtMoney(totals.payment_due)}</td>
        </tr>
      `
      : "";

    els.payOverModalContent.innerHTML = `
      ${paymentDueNote}
      <div class="table-wrapper">
        <table class="data-table cashbills-payover-table">
          <colgroup>
            <col style="width:200px" />
            <col style="width:120px" />
            <col style="width:140px" />
            <col style="width:120px" />
            <col style="width:140px" />
            <col style="width:130px" />
            <col style="width:140px" />
            <col style="width:120px" />
            <col style="width:140px" />
          </colgroup>
          <thead>
            <tr>
              <th>Description</th>
              <th>Plan start</th>
              <th class="num">Original principal</th>
              <th class="num">Total payments</th>
              <th class="num">Remaining principal</th>
              <th class="num">Remaining payments</th>
              <th class="num">Monthly principal</th>
              <th class="num">Monthly fee</th>
              <th class="num">Payment due</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
            ${totalsRow}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderCalendarModal = () => {
    if (!els.calendarModal) return;
    els.calendarModal.classList.toggle("is-open", state.calendarModalOpen);
    els.calendarModal.setAttribute("aria-hidden", state.calendarModalOpen ? "false" : "true");
    if (!state.calendarModalOpen || !els.calendarModalContent) return;

    const context = state.calendarModalContext || {};
    const titleLabel =
      context.year != null && context.monthIndex != null && context.day != null
        ? formatDateFromParts(context.year, context.monthIndex, context.day)
        : "Items due";
    if (els.calendarModalTitle) {
      els.calendarModalTitle.textContent = titleLabel && titleLabel !== "—" ? `Due on ${titleLabel}` : "Items due";
    }

    const items = Array.isArray(state.calendarModalItems) ? state.calendarModalItems : [];
    if (!items.length) {
      els.calendarModalContent.innerHTML = `<div class="ui-muted">No items due on this day.</div>`;
      return;
    }

    const total = items.reduce((sum, item) => sum + Number(item.amount || 0), 0);
    const tableRows = items
      .map((item) => {
        const label = item.label || "—";
        const subLabel = item.subLabel ? `<div class="cashbills-modal__cell-muted">${item.subLabel}</div>` : "";
        return `
          <tr>
            <td>${item.type || "—"}</td>
            <td>
              <div>${label}</div>
              ${subLabel}
            </td>
            <td class="num ui-tabular-nums">${currency.format(item.amount || 0)}</td>
          </tr>
        `;
      })
      .join("");

    els.calendarModalContent.innerHTML = `
      <div class="ui-muted" style="margin-bottom:8px">Total due: ${currency.format(total)}</div>
      <div class="table-wrapper">
        <table class="data-table cashbills-calendar-detail-table">
          <colgroup>
            <col style="width:160px" />
            <col />
            <col style="width:160px" />
          </colgroup>
          <thead>
            <tr>
              <th>Type</th>
              <th>Item</th>
              <th class="num">Amount</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const loadFinanceTransactions = (context) => {
    if (!context || !context.year || !context.month || !context.account_id) return;
    state.financeModalLoading = true;
    state.financeModalError = null;
    state.financeModalRows = [];
    renderFinanceModal();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      year: String(context.year),
      month: String(context.month),
      account_id: String(context.account_id),
    });
    return fetchJson(`/api/cash-bills/card-finance/transactions?${params}`)
      .then((payload) => {
        state.financeModalLoading = false;
        state.financeModalRows = payload.rows || [];
        renderFinanceModal();
      })
      .catch((err) => {
        state.financeModalLoading = false;
        state.financeModalError = err ? String(err) : COPY.finance.detailError;
        renderFinanceModal();
      });
  };

  const loadDepositTransactions = (context) => {
    if (!context || !context.year || !context.month || !context.account_id) return;
    state.depositsModalLoading = true;
    state.depositsModalError = null;
    state.depositsModalRows = [];
    renderDepositsModal();
    const params = new URLSearchParams({
      account_id: String(context.account_id),
      year: String(context.year),
      month: String(context.month),
    });
    return fetchJson(`/api/cash-bills/deposits/transactions?${params}`)
      .then((payload) => {
        state.depositsModalLoading = false;
        state.depositsModalRows = payload.rows || [];
        state.depositsModalContext = {
          ...context,
          account_label: payload.account_label || context.account_label,
        };
        renderDepositsModal();
      })
      .catch((err) => {
        state.depositsModalLoading = false;
        state.depositsModalError = err ? String(err) : COPY.deposits.detailError;
        renderDepositsModal();
      });
  };

  const openFinanceModal = (context) => {
    state.financeModalOpen = true;
    state.financeModalContext = context || null;
    loadFinanceTransactions(context || {});
  };

  const closeFinanceModal = () => {
    state.financeModalOpen = false;
    state.financeModalContext = null;
    state.financeModalRows = [];
    state.financeModalLoading = false;
    state.financeModalError = null;
    renderFinanceModal();
  };

  const openDepositsModal = (context) => {
    state.depositsModalOpen = true;
    state.depositsModalContext = context || null;
    renderDepositsModal();
    loadDepositTransactions(context || {});
  };

  const closeDepositsModal = () => {
    state.depositsModalOpen = false;
    state.depositsModalContext = null;
    state.depositsModalRows = [];
    state.depositsModalLoading = false;
    state.depositsModalError = null;
    renderDepositsModal();
  };

  const openCalendarModal = (context) => {
    if (!context || context.year == null || context.monthIndex == null || context.day == null) return;
    const asOfDate = clampDate(state.asOfDate);
    const scopedCardBills = state.data ? applyScope(state.data.bills || [], state.scope) : [];
    state.calendarModalItems = collectCalendarDayItems(
      state.recurringBills || [],
      state.cardRecurringCharges || [],
      scopedCardBills,
      asOfDate,
      context.year,
      context.monthIndex,
      context.day
    );
    state.calendarModalContext = context;
    state.calendarModalOpen = true;
    renderCalendarModal();
  };

  const closeCalendarModal = () => {
    state.calendarModalOpen = false;
    state.calendarModalContext = null;
    state.calendarModalItems = [];
    renderCalendarModal();
  };

  const openPayOverModal = (bill) => {
    if (!bill || !bill.pay_over_time || !Array.isArray(bill.pay_over_time.rows) || !bill.pay_over_time.rows.length) return;
    state.payOverModalOpen = true;
    state.payOverModalContext = {
      card_label: formatCardLabel(bill.card_name, bill.last4),
      pay_over_time: bill.pay_over_time,
    };
    renderPayOverModal();
  };

  const closePayOverModal = () => {
    state.payOverModalOpen = false;
    state.payOverModalContext = null;
    renderPayOverModal();
  };

  const renderCash = (rows) => {
    if (!els.cashTable) return;
    if (state.cashError) {
      els.cashTable.innerHTML = `
        <div class="alert alert--warn">
          <div>${COPY.error.accounts}</div>
          <div class="ui-muted" style="margin-top:4px">${state.cashError}</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryAccounts">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryAccounts");
      if (retry) retry.addEventListener("click", () => loadData());
      return;
    }
    if (!rows.length) {
      els.cashTable.innerHTML = `
        <div class="ui-muted">${COPY.empty.accounts}</div>
        <div class="ui-muted" style="margin-top:4px">${COPY.empty.accountsHelper}</div>
      `;
      return;
    }
    const tableRows = rows
      .map((c) => {
        return `
          <tr>
            <td>
              <div>${c.account_name}</div>
            </td>
            <td class="num ui-tabular-nums">${currency.format(c.available_balance || 0)}</td>
            <td class="num ui-tabular-nums">${c.current_balance != null ? currency.format(c.current_balance) : "—"}</td>
          </tr>
        `;
      })
      .join("");
    els.cashTable.innerHTML = `
      <div class="table-wrapper">
        <table class="data-table cashbills-cash-table">
          <colgroup>
            <col />
            <col style="width:150px" />
            <col style="width:150px" />
          </colgroup>
          <thead>
            <tr>
              <th>Account</th>
              <th class="num" title="Balance available to spend, as reported by your institution.">Available</th>
              <th class="num">Current</th>
            </tr>
          </thead>
          <tbody>
            ${tableRows}
          </tbody>
        </table>
      </div>
    `;
  };

  const renderCoverage = (summary, rangeDays, projected) => {
    if (!els.coverage) return;
    const status = coverageStatus(summary.cashTotal, summary.dueTotal);
    const badgeClass = status === "covered" ? "ui-badge--safe" : status === "tight" ? "ui-badge--risk" : "ui-badge--bad";
    const helper =
      status === "covered"
        ? COPY.coverage.covered
        : status === "tight"
        ? COPY.coverage.tight
        : COPY.coverage.shortfall;
    const projectedReady = projected && projected.ready;
    const projectedStatus = projectedReady ? coverageStatus(summary.cashTotal, projected.total) : "covered";
    const projectedBadge =
      projectedStatus === "covered" ? "OK" : projectedStatus === "tight" ? "Tight" : "Shortfall";
    const projectedBadgeClass =
      projectedStatus === "covered" ? "ui-badge--safe" : projectedStatus === "tight" ? "ui-badge--risk" : "ui-badge--bad";
    els.coverage.innerHTML = `
      <div class="cashbills-coverage">
        <div class="ui-muted">Next ${rangeDays} days</div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Card bills due</span>
          <span class="ui-tabular-nums">${currency.format(summary.cardDueTotal)}</span>
        </div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Monthly bills due</span>
          <span class="ui-tabular-nums">${currency.format(summary.recurringDueTotal)}</span>
        </div>
        <div class="cashbills-coverage__divider"></div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted" title="${COPY.coverage.billsDueTooltip}">Bills due</span>
          <span class="ui-tabular-nums">${currency.format(summary.dueTotal)}</span>
        </div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Cash available</span>
          <span class="ui-tabular-nums">${currency.format(summary.cashTotal)}</span>
        </div>
        <div class="cashbills-coverage__row cashbills-coverage__net">
          <span>Net</span>
          <span class="ui-tabular-nums">${currency.format(summary.netAfter)}</span>
        </div>
        <div style="margin-top:6px">
          <span class="ui-badge ${badgeClass}">${status === "covered" ? "Covered" : status === "tight" ? "Tight" : "Shortfall"}</span>
        </div>
        <div class="ui-muted" style="margin-top:6px">${helper}</div>
        <div class="cashbills-coverage__divider"></div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted" title="${COPY.coverage.projectedTooltip}">Projected recurring outflows</span>
          <span></span>
        </div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Projected card bills</span>
          <span class="ui-tabular-nums">${projectedReady ? currency.format(projected.cardBills) : "—"}</span>
        </div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Projected monthly bills</span>
          <span class="ui-tabular-nums">${projectedReady ? currency.format(projected.monthlyBills) : "—"}</span>
        </div>
        <div class="cashbills-coverage__row">
          <span class="ui-muted">Projected monthly card charges</span>
          <span class="ui-tabular-nums">${projectedReady ? currency.format(projected.monthlyCardCharges) : "—"}</span>
        </div>
        <div class="cashbills-coverage__row cashbills-coverage__net">
          <span>Projected total</span>
          <span class="ui-tabular-nums">${projectedReady ? currency.format(projected.total) : "—"}</span>
        </div>
        <div style="margin-top:6px">
          ${projectedReady ? `<span class="ui-badge ${projectedBadgeClass}">${projectedBadge}</span>` : `<span class="ui-badge ui-badge--neutral">—</span>`}
        </div>
        <div class="ui-muted" style="margin-top:4px">Based on checking balances as of ${formatDate(state.asOfDate)}.</div>
      </div>
    `;
    if (els.coverageTitle) {
      els.coverageTitle.textContent = `${COPY.coverage.title}`;
    }
  };

  const setLoading = () => {
    if (els.kpis) {
      els.kpis.innerHTML = Array.from({ length: 4 })
        .map(() => `<div class="ui-card ui-kpi ui-skeleton"></div>`)
        .join("");
    }
    if (els.billsTable) {
      els.billsTable.innerHTML = `<div class="ui-muted">${COPY.loading.bills}</div>`;
    }
    if (els.cardRecurringTable) {
      els.cardRecurringTable.innerHTML = `<div class="ui-muted">${COPY.cardRecurring.loading}</div>`;
    }
    if (els.financeTable) {
      els.financeTable.innerHTML = `<div class="ui-muted">${COPY.finance.loading}</div>`;
    }
    if (els.monthlyTotals) {
      els.monthlyTotals.innerHTML = `<div class="ui-muted">—</div>`;
    }
    if (els.cashTable) {
      els.cashTable.innerHTML = `<div class="ui-muted">${COPY.loading.accounts}</div>`;
    }
    if (els.recurringTable) {
      els.recurringTable.innerHTML = `<div class="ui-muted">${COPY.recurring.loading}</div>`;
    }
    if (els.coverage) {
      els.coverage.innerHTML = `<div class="ui-skeleton ui-skeleton--block"></div>`;
    }
  };

  const setError = (message) => {
    const billsHtml = `<div class="alert alert--warn">
      <div>${COPY.error.bills}</div>
      <button class="btn btn--secondary" type="button" id="cashBillsRetryBills">${COPY.error.retry}</button>
    </div>`;
    const accountsHtml = `<div class="alert alert--warn">
      <div>${COPY.error.accounts}</div>
      <button class="btn btn--secondary" type="button" id="cashBillsRetryAccounts">${COPY.error.retry}</button>
    </div>`;
    if (els.billsTable) els.billsTable.innerHTML = billsHtml;
    if (els.cashTable) els.cashTable.innerHTML = accountsHtml;
    if (els.coverage) els.coverage.innerHTML = `<div class="alert alert--warn">${message}</div>`;
    if (els.kpis) {
      els.kpis.innerHTML = "";
    }
    if (els.monthlyTotals) {
      els.monthlyTotals.innerHTML = `<div class="ui-muted">—</div>`;
    }
    const retryBills = document.getElementById("cashBillsRetryBills");
    const retryAccounts = document.getElementById("cashBillsRetryAccounts");
    if (retryBills) retryBills.addEventListener("click", () => loadData());
    if (retryAccounts) retryAccounts.addEventListener("click", () => loadData());
  };

  const renderRecurringSection = () => {
    const asOfDate = clampDate(state.asOfDate);
    if (state.recurringLoading || state.recurringError) {
      renderRecurring([], asOfDate);
      if (els.recurringSummary) {
        els.recurringSummary.textContent = "—";
      }
      renderMonthlyTotals();
      return;
    }
    const bills = filterRecurringBills(state.recurringBills || [], asOfDate, state.rangeDays, state.status);
    renderRecurring(bills, asOfDate);
    if (els.recurringSummary) {
      els.recurringSummary.textContent = bills.length === 0 ? "No bills in range" : `${bills.length} bill${bills.length === 1 ? "" : "s"} in range`;
    }
    renderMonthlyTotals();
  };

  const renderCardRecurringSection = () => {
    const asOfDate = clampDate(state.asOfDate);
    if (state.cardRecurringLoading || state.cardRecurringError) {
      renderCardRecurring([], asOfDate);
      if (els.cardRecurringSummary) {
        els.cardRecurringSummary.textContent = "—";
      }
      renderMonthlyTotals();
      return;
    }
    const charges = filterRecurringBills(state.cardRecurringCharges || [], asOfDate, state.rangeDays, state.status);
    renderCardRecurring(charges, asOfDate);
    if (els.cardRecurringSummary) {
      els.cardRecurringSummary.textContent =
        charges.length === 0 ? "No charges in range" : `${charges.length} charge${charges.length === 1 ? "" : "s"} in range`;
    }
    renderMonthlyTotals();
  };

  const render = () => {
    if (!state.data) return;
    const asOfDate = clampDate(state.asOfDate);
    const scopedBills = applyScope(state.data.bills, state.scope);
    const scopedCash = applyScope(state.data.cash_accounts, state.scope);
    const bills = filterBills(scopedBills, asOfDate, state.rangeDays, state.status);
    const summary = computeSummary(scopedBills, scopedCash, asOfDate, state.rangeDays, state.recurringDueTotal);
    const projectedReady =
      !state.recurringLoading &&
      !state.cardRecurringLoading &&
      !state.recurringError &&
      !state.cardRecurringError;
    const projected = computeProjectedTotals(
      scopedBills,
      state.recurringBills || [],
      state.cardRecurringCharges || [],
      asOfDate,
      state.rangeDays,
      projectedReady
    );
    renderKpis(summary, state.rangeDays);
    renderBills(bills, asOfDate);
    renderCardRecurringSection();
    renderFinanceSection();
    renderCash(scopedCash);
    renderCoverage(summary, state.rangeDays, projected);
    renderRecurringSection();
    if (els.billsSummary) {
      els.billsSummary.textContent = bills.length === 0 ? "No bills in range" : `${bills.length} bill${bills.length === 1 ? "" : "s"} in range`;
    }
    if (els.billsTitle) {
      els.billsTitle.textContent = state.status === "paid" || state.status === "all" ? "Card bills" : "Upcoming card bills";
    }
    if (els.cashSummary) {
      els.cashSummary.textContent = `${scopedCash.length} account${scopedCash.length === 1 ? "" : "s"}`;
    }
    if (els.context) {
      const asOfLabel = formatDate(state.asOfDate);
      const scopeLabel = scopeLabelText(state.scope);
      els.context.textContent = `As of ${asOfLabel} · Scope: ${scopeLabel}`;
    }
  };

  const useDashboardData = () =>
    new Promise((resolve, reject) => {
      setTimeout(() => {
        try {
          if (dataSeed && dataSeed.error) {
            throw new Error(dataSeed.error);
          }
          resolve(mergeCardBalances(JSON.parse(JSON.stringify(dataSeed))));
        } catch (err) {
          reject(err);
        }
      }, 350);
    });

  const fetchJson = (url, options = {}) =>
    fetch(url, {
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", ...(options.headers || {}) },
      ...options,
    }).then(async (res) => {
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `${res.status}`);
      }
      return res.json();
    });

  const refreshRecurringSummary = () => {
    state.recurringLoading = true;
    state.recurringError = null;
    renderRecurringSection();
    refreshCardRecurringSummary();
    refreshFinanceCharges();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      range_days: String(state.rangeDays),
    });
    return fetchJson(`/api/cash-bills/recurring/summary?${params}`)
      .then((data) => {
        state.recurringLoading = false;
        state.recurringBills = Array.isArray(data.bills) ? data.bills : [];
        state.recurringDueTotal = Number(data.due_total || 0);
        state.recurringError = null;
        if (state.data) {
          render();
        } else {
          renderRecurringSection();
        }
      })
      .catch((err) => {
        state.recurringLoading = false;
        state.recurringError = err ? String(err) : "Failed";
        state.recurringBills = [];
        state.recurringDueTotal = 0;
        if (state.data) {
          render();
        } else {
          renderRecurringSection();
        }
      });
  };

  const refreshDepositSummary = (force = false) => {
    if (state.depositsLoading && !force) return;
    state.depositsLoading = true;
    state.depositsError = null;
    renderMonthlyTotals();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      months: String(state.depositsMonths || 6),
    });
    if (state.depositsAccountId) {
      params.append("account_id", String(state.depositsAccountId));
    }
    return fetchJson(`/api/cash-bills/deposits/summary?${params}`)
      .then((data) => {
        state.depositsLoading = false;
        state.depositsError = null;
        state.depositsAccounts = Array.isArray(data.accounts) ? data.accounts : [];
        state.depositsMonthly = Array.isArray(data.monthly) ? data.monthly : [];
        if (data.months) {
          state.depositsMonths = Number(data.months) || state.depositsMonths;
        }
        const selected = data.selected_account_id != null ? String(data.selected_account_id) : "";
        if (!state.depositsAccountId || !state.depositsAccounts.some((acct) => String(acct.id) === String(state.depositsAccountId))) {
          state.depositsAccountId = selected || (state.depositsAccounts[0] ? String(state.depositsAccounts[0].id) : null);
        }
        renderMonthlyTotals();
      })
      .catch((err) => {
        state.depositsLoading = false;
        state.depositsError = err ? String(err) : "Failed";
        state.depositsAccounts = [];
        state.depositsMonthly = [];
        renderMonthlyTotals();
      });
  };

  const refreshCardRecurringSummary = () => {
    state.cardRecurringLoading = true;
    state.cardRecurringError = null;
    renderCardRecurringSection();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
    });
    return fetchJson(`/api/cash-bills/card-recurring/summary?${params}`)
      .then((data) => {
        state.cardRecurringLoading = false;
        state.cardRecurringCharges = Array.isArray(data.charges) ? data.charges : [];
        state.cardRecurringError = null;
        renderCardRecurringSection();
      })
      .catch((err) => {
        state.cardRecurringLoading = false;
        state.cardRecurringError = err ? String(err) : "Failed";
        state.cardRecurringCharges = [];
        renderCardRecurringSection();
      });
  };

  const refreshFinanceCharges = () => {
    state.financeLoading = true;
    state.financeError = null;
    renderFinanceSection();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      months: String(state.financeMonths || 12),
    });
    return fetchJson(`/api/cash-bills/card-finance?${params}`)
      .then((data) => {
        state.financeLoading = false;
        state.financeError = null;
        state.financeRows = Array.isArray(data.rows) ? data.rows : [];
        if (data.months) {
          state.financeMonths = Number(data.months) || state.financeMonths;
        }
        renderFinanceSection();
      })
      .catch((err) => {
        state.financeLoading = false;
        state.financeError = err ? String(err) : "Failed";
        state.financeRows = [];
        renderFinanceSection();
      });
  };

  const loadSuggestions = (force = false) => {
    if (state.suggestionsLoading) return;
    if (state.suggestions.length && !force) {
      renderModal();
      return;
    }
    state.suggestionsLoading = true;
    state.suggestionsError = null;
    renderModal();
    const params = new URLSearchParams({ as_of: state.asOfDate, scope: state.scope });
    fetchJson(`/api/cash-bills/recurring/suggestions?${params}`)
      .then((data) => {
        state.suggestionsLoading = false;
        state.suggestionsError = null;
        state.suggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
        renderModal();
      })
      .catch((err) => {
        state.suggestionsLoading = false;
        state.suggestionsError = err ? String(err) : "Failed";
        renderModal();
      });
  };

  const loadCardSuggestions = (force = false) => {
    if (state.cardSuggestionsLoading) return;
    if (state.cardSuggestions.length && !force) {
      renderCardModal();
      return;
    }
    state.cardSuggestionsLoading = true;
    state.cardSuggestionsError = null;
    renderCardModal();
    const params = new URLSearchParams({ as_of: state.asOfDate, scope: state.scope });
    fetchJson(`/api/cash-bills/card-recurring/suggestions?${params}`)
      .then((data) => {
        state.cardSuggestionsLoading = false;
        state.cardSuggestionsError = null;
        state.cardSuggestions = Array.isArray(data.suggestions) ? data.suggestions : [];
        renderCardModal();
      })
      .catch((err) => {
        state.cardSuggestionsLoading = false;
        state.cardSuggestionsError = err ? String(err) : "Failed";
        renderCardModal();
      });
  };

  const loadRecurringAll = () => {
    if (state.recurringAllLoading) return;
    state.recurringAllLoading = true;
    state.recurringAllError = null;
    renderModal();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      range_days: String(state.rangeDays),
      include_inactive: "1",
    });
    fetchJson(`/api/cash-bills/recurring/summary?${params}`)
      .then((data) => {
        state.recurringAllLoading = false;
        state.recurringAllError = null;
        state.recurringAllBills = Array.isArray(data.bills) ? data.bills : [];
        renderModal();
      })
      .catch((err) => {
        state.recurringAllLoading = false;
        state.recurringAllError = err ? String(err) : "Failed";
        renderModal();
      });
  };

  const loadCardRecurringAll = () => {
    if (state.cardRecurringAllLoading) return;
    state.cardRecurringAllLoading = true;
    state.cardRecurringAllError = null;
    renderCardModal();
    const params = new URLSearchParams({
      as_of: state.asOfDate,
      scope: state.scope,
      include_inactive: "1",
    });
    fetchJson(`/api/cash-bills/card-recurring/summary?${params}`)
      .then((data) => {
        state.cardRecurringAllLoading = false;
        state.cardRecurringAllError = null;
        state.cardRecurringAllCharges = Array.isArray(data.charges) ? data.charges : [];
        renderCardModal();
      })
      .catch((err) => {
        state.cardRecurringAllLoading = false;
        state.cardRecurringAllError = err ? String(err) : "Failed";
        renderCardModal();
      });
  };

  const loadRecent = () => {
    if (state.recentLoading) return;
    state.recentLoading = true;
    state.recentError = null;
    renderModal();
    const params = new URLSearchParams({ as_of: state.asOfDate, scope: state.scope, days: "30" });
    fetchJson(`/api/cash-bills/recurring/recent?${params}`)
      .then((data) => {
        state.recentLoading = false;
        state.recentError = null;
        state.recentCharges = Array.isArray(data.charges) ? data.charges : [];
        renderModal();
      })
      .catch((err) => {
        state.recentLoading = false;
        state.recentError = err ? String(err) : "Failed";
        renderModal();
      });
  };

  const loadCardRecent = () => {
    if (state.cardRecentLoading) return;
    state.cardRecentLoading = true;
    state.cardRecentError = null;
    renderCardModal();
    const params = new URLSearchParams({ as_of: state.asOfDate, scope: state.scope, days: "30" });
    fetchJson(`/api/cash-bills/card-recurring/recent?${params}`)
      .then((data) => {
        state.cardRecentLoading = false;
        state.cardRecentError = null;
        state.cardRecentCharges = Array.isArray(data.charges) ? data.charges : [];
        renderCardModal();
      })
      .catch((err) => {
        state.cardRecentLoading = false;
        state.cardRecentError = err ? String(err) : "Failed";
        renderCardModal();
      });
  };

  const activateSuggestion = (payload) =>
    fetchJson("/api/cash-bills/recurring/activate", { method: "POST", body: JSON.stringify(payload) });

  const ignoreSuggestion = (payload) =>
    fetchJson("/api/cash-bills/recurring/ignore", { method: "POST", body: JSON.stringify(payload) });

  const updateBill = (billId, payload) =>
    fetchJson(`/api/cash-bills/recurring/${billId}`, { method: "PATCH", body: JSON.stringify(payload) });

  const activateCardSuggestion = (payload) =>
    fetchJson("/api/cash-bills/card-recurring/activate", { method: "POST", body: JSON.stringify(payload) });

  const ignoreCardSuggestion = (payload) =>
    fetchJson("/api/cash-bills/card-recurring/ignore", { method: "POST", body: JSON.stringify(payload) });

  const updateCardCharge = (chargeId, payload) =>
    fetchJson(`/api/cash-bills/card-recurring/${chargeId}`, { method: "PATCH", body: JSON.stringify(payload) });

  const renderModalSuggestions = () => {
    if (!els.modalContent) return;
    if (state.suggestionsLoading) {
      els.modalContent.innerHTML = `<div class="ui-muted">Loading suggestions…</div>`;
      return;
    }
    if (state.suggestionsError) {
      els.modalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load suggestions.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetrySuggestions">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetrySuggestions");
      if (retry) retry.addEventListener("click", () => loadSuggestions(true));
      return;
    }
    if (!state.suggestions.length) {
      els.modalContent.innerHTML = `
        <div class="ui-muted">${COPY.recurring.suggestedEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">${COPY.recurring.suggestedHelper}</div>
      `;
      return;
    }
    const rows = state.suggestions
      .map((item, idx) => {
        const isEditing = state.modalEdit && state.modalEdit.kind === "suggestion" && state.modalEdit.index === idx;
        const conf = Math.round((item.confidence || 0) * 100);
        const confClass = conf >= 75 ? "ui-badge--safe" : conf >= 50 ? "ui-badge--risk" : "ui-badge--neutral";
        const dueDay = item.due_day_of_month ? `Day ${item.due_day_of_month}` : "—";
        const lastSeen = item.last_seen_date ? formatDate(item.last_seen_date) : "—";
        const amountLabel = formatSuggestionAmount(item);
        const merchantLabel = formatMerchantLabel(item.merchant_display, item.description_sample, item.name);
        const descLabel = item.description_sample || "—";
        const cardLabel = formatCardLabel(item.source_account_name, item.source_account_mask);
        const cardIdLabel = formatPlaidAccountId(item.plaid_account_id);
        const cardCell = cardLabel || cardIdLabel ? `<div>${cardLabel || "—"}</div>${cardIdLabel ? `<div class="cashbills-modal__cell-muted">Acct ${cardIdLabel}</div>` : ""}` : "—";
        const memberCell = formatMemberCell(item.cardholder_name, item.cardholder_source);
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="suggestion" data-index="${idx}">
              <label>Bill name<input class="bill-name" value="${item.name || ""}" /></label>
              <label>Due day<input type="number" min="1" max="31" class="bill-day" value="${item.due_day_of_month || ""}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED"${item.amount_mode === "FIXED" ? " selected" : ""}>Fixed</option>
                  <option value="RANGE"${item.amount_mode === "RANGE" ? " selected" : ""}>Range</option>
                  <option value="VARIABLE"${item.amount_mode === "VARIABLE" ? " selected" : ""}>Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${item.amount_expected || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="${item.amount_min || ""}" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="${item.amount_max || ""}" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-bill-save" data-index="${idx}">Save</button>
                <button class="btn btn--secondary js-bill-cancel" data-index="${idx}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row" data-bill-id="${bill.id}">
            <div class="cashbills-modal__grid">
              <div>
                <div class="cashbills-modal__cell-title">${item.name}</div>
                <div class="cashbills-modal__cell-muted">${item.occurrences} payments</div>
              </div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div>${amountLabel}</div>
              <div>${dueDay}</div>
              <div><span class="ui-badge ${confClass}">${conf}%</span></div>
              <div>${lastSeen}</div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-suggest-add" data-index="${idx}">Add</button>
                <button class="btn js-suggest-ignore" data-index="${idx}">Ignore</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.modalContent.innerHTML = `
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__cell-muted">
          <div>Bill</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Typical amount</div>
          <div>Typical day</div>
          <div>Confidence</div>
          <div>Last seen</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderModalActive = () => {
    if (!els.modalContent) return;
    if (state.recurringAllLoading) {
      els.modalContent.innerHTML = `<div class="ui-muted">Loading bills…</div>`;
      return;
    }
    if (state.recurringAllError) {
      els.modalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load bills.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryActive">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryActive");
      if (retry) retry.addEventListener("click", () => loadRecurringAll());
      return;
    }
    if (!state.recurringAllBills.length) {
      els.modalContent.innerHTML = `
        <div class="ui-muted">${COPY.recurring.activeEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">Review suggestions to add monthly bills.</div>
      `;
      return;
    }
    const rows = state.recurringAllBills
      .map((bill) => {
        const isEditing = state.modalEdit && state.modalEdit.kind === "active" && state.modalEdit.billId === bill.id;
        const dueDay = bill.due_day_of_month ? `Day ${bill.due_day_of_month}` : "—";
        const expected = formatExpectedDisplay(bill);
        const lastPaymentDate = bill.last_payment_date ? formatDate(bill.last_payment_date) : "—";
        const lastPaymentAmt = bill.last_payment_amount != null ? currency.format(bill.last_payment_amount) : "—";
        const merchantLabel = bill.merchant_display || bill.name || "—";
        const descLabel = bill.description_sample || "—";
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="active" data-bill-id="${bill.id}">
              <label>Bill name<input class="bill-name" value="${bill.name || ""}" /></label>
              <label>Due day<input type="number" min="1" max="31" class="bill-day" value="${bill.due_day_of_month || ""}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED"${bill.amount_mode === "FIXED" ? " selected" : ""}>Fixed</option>
                  <option value="RANGE"${bill.amount_mode === "RANGE" ? " selected" : ""}>Range</option>
                  <option value="VARIABLE"${bill.amount_mode === "VARIABLE" ? " selected" : ""}>Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${bill.amount_expected || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="${bill.amount_min || ""}" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="${bill.amount_max || ""}" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-bill-update" data-bill-id="${bill.id}">Save</button>
                <button class="btn btn--secondary js-bill-cancel" data-bill-id="${bill.id}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row" data-bill-id="${bill.id}">
            <div class="cashbills-modal__grid cashbills-modal__grid--active">
              <div>
                <div class="cashbills-modal__cell-title">${bill.name}</div>
                <div class="cashbills-modal__cell-muted">${bill.scope || "PERSONAL"}</div>
              </div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div>${dueDay}</div>
              <div>${expected}</div>
              <div>
                <div>${lastPaymentDate}</div>
                <div class="cashbills-modal__cell-muted ui-tabular-nums">${lastPaymentAmt}</div>
              </div>
              <div>
                <label class="cashbills-modal__toggle">
                  <input type="checkbox" class="js-bill-toggle" data-bill-id="${bill.id}" ${bill.is_active ? "checked" : ""} />
                  Active
                </label>
              </div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-bill-edit" data-bill-id="${bill.id}">Edit</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.modalContent.innerHTML = `
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__grid--active cashbills-modal__cell-muted">
          <div>Bill</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Due day</div>
          <div>Expected</div>
          <div>Last payment</div>
          <div>Active</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderModalRecent = () => {
    if (!els.modalContent) return;
    if (state.recentLoading) {
      els.modalContent.innerHTML = `<div class="ui-muted">Loading recent charges…</div>`;
      return;
    }
    if (state.recentError) {
      els.modalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load recent charges.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryRecent">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryRecent");
      if (retry) retry.addEventListener("click", () => loadRecent());
      return;
    }
    if (!state.recentCharges.length) {
      els.modalContent.innerHTML = `
        <div class="ui-muted">${COPY.recurring.recentEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">${COPY.recurring.recentHelper}</div>
      `;
      return;
    }
    const rows = state.recentCharges
      .map((item, idx) => {
        const isEditing = state.modalEdit && state.modalEdit.kind === "recent" && state.modalEdit.index === idx;
        const posted = item.posted_date ? formatDate(item.posted_date) : "—";
        const amount = item.amount != null ? currency.format(item.amount) : "—";
        const merchantLabel = formatMerchantLabel(item.merchant_display, item.description_sample, item.name);
        const descLabel = item.description_sample || "—";
        const accountLabel = formatCardLabel(item.source_account_name, item.source_account_mask);
        const accountIdLabel = formatPlaidAccountId(item.plaid_account_id);
        const accountCell =
          accountLabel || accountIdLabel
            ? `<div>${accountLabel || "—"}</div>${accountIdLabel ? `<div class="cashbills-modal__cell-muted">Acct ${accountIdLabel}</div>` : ""}`
            : "—";
        const dueDefault = item.posted_date ? String(new Date(item.posted_date).getDate()) : "";
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="recent" data-index="${idx}">
              <label>Bill name<input class="bill-name" value="${merchantLabel}" /></label>
              <label>Due day<input type="number" min="1" max="31" class="bill-day" value="${dueDefault}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED" selected>Fixed</option>
                  <option value="RANGE">Range</option>
                  <option value="VARIABLE">Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${item.amount || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-recent-save" data-index="${idx}">Save</button>
                <button class="btn btn--secondary js-bill-cancel" data-index="${idx}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row">
            <div class="cashbills-modal__grid cashbills-modal__grid--recent">
              <div>${posted}</div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div>${amount}</div>
              <div class="cashbills-modal__cell-wrap">${accountCell}</div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-recent-add" data-index="${idx}">Add</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.modalContent.innerHTML = `
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__grid--recent cashbills-modal__cell-muted">
          <div>Date</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Amount</div>
          <div>Account</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderModal = () => {
    if (!els.modal) return;
    els.modal.classList.toggle("is-open", state.modalOpen);
    els.modal.setAttribute("aria-hidden", state.modalOpen ? "false" : "true");
    if (!state.modalOpen) return;
    if (els.modalTabs) {
      Array.from(els.modalTabs.querySelectorAll("button")).forEach((btn) => {
        const isActive = btn.getAttribute("data-tab") === state.modalTab;
        btn.classList.toggle("ui-tab--active", isActive);
        btn.setAttribute("aria-pressed", isActive ? "true" : "false");
      });
    }
    if (state.modalTab === "suggested") {
      renderModalSuggestions();
    } else if (state.modalTab === "active") {
      renderModalActive();
    } else {
      renderModalRecent();
    }
    if (state.modalFocusBillId && state.modalTab === "active") {
      const focusId = state.modalFocusBillId;
      setTimeout(() => {
        const row = els.modalContent?.querySelector(`[data-bill-id="${focusId}"]`);
        if (!row) return;
        row.classList.add("is-focus");
        row.scrollIntoView({ block: "center", behavior: "smooth" });
        setTimeout(() => row.classList.remove("is-focus"), 2000);
        state.modalFocusBillId = null;
      }, 50);
    }
  };

  const renderCardModalSuggestions = () => {
    if (!els.cardModalContent) return;
    if (state.cardSuggestionsLoading) {
      els.cardModalContent.innerHTML = `<div class="ui-muted">Loading suggestions…</div>`;
      return;
    }
    if (state.cardSuggestionsError) {
      els.cardModalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load suggestions.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryCardSuggestions">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryCardSuggestions");
      if (retry) retry.addEventListener("click", () => loadCardSuggestions(true));
      return;
    }
    const filtered = state.cardSuggestions
      .map((item, idx) => ({ item, idx }))
      .filter(({ item }) => matchesMemberFilter(item) && matchesAccountFilter(item));
    if (!filtered.length) {
      els.cardModalContent.innerHTML = `
        <div class="ui-muted">${COPY.cardRecurring.suggestedEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">${COPY.cardRecurring.suggestedHelper}</div>
      `;
      return;
    }
    const rows = filtered
      .map(({ item, idx }) => {
        const isEditing = state.cardModalEdit && state.cardModalEdit.kind === "suggestion" && state.cardModalEdit.index === idx;
        const conf = Math.round((item.confidence || 0) * 100);
        const confClass = conf >= 75 ? "ui-badge--safe" : conf >= 50 ? "ui-badge--risk" : "ui-badge--neutral";
        const dueDay = item.due_day_of_month ? `Day ${item.due_day_of_month}` : "—";
        const lastSeen = item.last_seen_date ? formatDate(item.last_seen_date) : "—";
        const amountLabel = formatSuggestionAmount(item);
        const merchantLabel = formatMerchantLabel(item.merchant_display, item.description_sample, item.name);
        const descLabel = item.description_sample || "—";
        const chargeLabel = formatChargeLabel(item.name, item.merchant_display, item.description_sample);
        const cardLabel = formatCardLabel(item.source_account_name, item.source_account_mask);
        const cardIdLabel = formatPlaidAccountId(item.plaid_account_id);
        const cardCell = cardLabel || cardIdLabel ? `<div>${cardLabel || "—"}</div>${cardIdLabel ? `<div class="cashbills-modal__cell-muted">Acct ${cardIdLabel}</div>` : ""}` : "—";
        const memberCell = formatMemberCell(item.cardholder_name, item.cardholder_source);
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="card-suggestion" data-index="${idx}">
              <label>Charge name<input class="bill-name" value="${item.name || ""}" /></label>
              <label>Charge day<input type="number" min="1" max="31" class="bill-day" value="${item.due_day_of_month || ""}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED"${item.amount_mode === "FIXED" ? " selected" : ""}>Fixed</option>
                  <option value="RANGE"${item.amount_mode === "RANGE" ? " selected" : ""}>Range</option>
                  <option value="VARIABLE"${item.amount_mode === "VARIABLE" ? " selected" : ""}>Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${item.amount_expected || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="${item.amount_min || ""}" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="${item.amount_max || ""}" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-card-save" data-index="${idx}">Save</button>
                <button class="btn btn--secondary js-card-cancel" data-index="${idx}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row" data-card-id="${item.id}">
            <div class="cashbills-modal__grid cashbills-modal__grid--card">
              <div>
                <div class="cashbills-modal__cell-title">${chargeLabel}</div>
                <div class="cashbills-modal__cell-muted">${item.occurrences} charges</div>
              </div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div class="cashbills-modal__cell-wrap">${cardCell}</div>
              <div class="cashbills-modal__cell-wrap">${memberCell}</div>
              <div>${amountLabel}</div>
              <div>${dueDay}</div>
              <div><span class="ui-badge ${confClass}">${conf}%</span></div>
              <div>${lastSeen}</div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-card-add" data-index="${idx}">Add</button>
                <button class="btn js-card-ignore" data-index="${idx}">Ignore</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.cardModalContent.innerHTML = `
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__grid--card cashbills-modal__cell-muted">
          <div>Charge</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Card</div>
          <div>Member</div>
          <div>Typical amount</div>
          <div>Typical day</div>
          <div>Confidence</div>
          <div>Last seen</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderCardModalActive = () => {
    if (!els.cardModalContent) return;
    if (state.cardRecurringAllLoading) {
      els.cardModalContent.innerHTML = `<div class="ui-muted">Loading charges…</div>`;
      return;
    }
    if (state.cardRecurringAllError) {
      els.cardModalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load charges.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryCardActive">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryCardActive");
      if (retry) retry.addEventListener("click", () => loadCardRecurringAll());
      return;
    }
    const filtered = applyCardFilters(state.cardRecurringAllCharges);
    if (!filtered.length) {
      els.cardModalContent.innerHTML = `
        <div class="ui-muted">${COPY.cardRecurring.activeEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">Review suggestions to add recurring card charges.</div>
      `;
      return;
    }
    const rows = filtered
      .map((charge) => {
        const isEditing = state.cardModalEdit && state.cardModalEdit.kind === "active" && state.cardModalEdit.chargeId === charge.id;
        const dueDay = charge.due_day_of_month ? `Day ${charge.due_day_of_month}` : "—";
        const expected = formatExpectedDisplay(charge);
        const lastChargeDate = charge.last_charge_date ? formatDate(charge.last_charge_date) : "—";
        const lastChargeAmt = charge.last_charge_amount != null ? currency.format(charge.last_charge_amount) : "—";
        const merchantLabel = formatMerchantLabel(charge.merchant_display, charge.description_sample, charge.name);
        const descLabel = charge.description_sample || "—";
        const chargeLabel = formatChargeLabel(charge.name, charge.merchant_display, charge.description_sample);
        const cardLabel = formatCardLabel(charge.source_account_name, charge.source_account_mask);
        const cardIdLabel = formatPlaidAccountId(charge.plaid_account_id);
        const cardCell = cardLabel || cardIdLabel ? `<div>${cardLabel || "—"}</div>${cardIdLabel ? `<div class="cashbills-modal__cell-muted">Acct ${cardIdLabel}</div>` : ""}` : "—";
        const memberCell = formatMemberCell(charge.cardholder_name, charge.cardholder_source);
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="card-active" data-charge-id="${charge.id}">
              <label>Charge name<input class="bill-name" value="${charge.name || ""}" /></label>
              <label>Charge day<input type="number" min="1" max="31" class="bill-day" value="${charge.due_day_of_month || ""}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED"${charge.amount_mode === "FIXED" ? " selected" : ""}>Fixed</option>
                  <option value="RANGE"${charge.amount_mode === "RANGE" ? " selected" : ""}>Range</option>
                  <option value="VARIABLE"${charge.amount_mode === "VARIABLE" ? " selected" : ""}>Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${charge.amount_expected || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="${charge.amount_min || ""}" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="${charge.amount_max || ""}" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-card-update" data-charge-id="${charge.id}">Save</button>
                <button class="btn btn--secondary js-card-cancel" data-charge-id="${charge.id}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row">
            <div class="cashbills-modal__grid cashbills-modal__grid--card-active">
              <div>
                <div class="cashbills-modal__cell-title">${chargeLabel}</div>
                <div class="cashbills-modal__cell-muted">${charge.scope || "PERSONAL"}</div>
              </div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div class="cashbills-modal__cell-wrap">${cardCell}</div>
              <div class="cashbills-modal__cell-wrap">${memberCell}</div>
              <div>${dueDay}</div>
              <div>${expected}</div>
              <div>
                <div>${lastChargeDate}</div>
                <div class="cashbills-modal__cell-muted ui-tabular-nums">${lastChargeAmt}</div>
              </div>
              <div>
                <label class="cashbills-modal__toggle">
                  <input type="checkbox" class="js-card-toggle" data-charge-id="${charge.id}" ${charge.is_active ? "checked" : ""} />
                  Active
                </label>
              </div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-card-edit" data-charge-id="${charge.id}">Edit</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.cardModalContent.innerHTML = `
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__grid--card-active cashbills-modal__cell-muted">
          <div>Charge</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Card</div>
          <div>Member</div>
          <div>Charge day</div>
          <div>Expected</div>
          <div>Last charge</div>
          <div>Active</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderCardModalRecent = () => {
    if (!els.cardModalContent) return;
    if (state.cardRecentLoading) {
      els.cardModalContent.innerHTML = `<div class="ui-muted">Loading recent charges…</div>`;
      return;
    }
    if (state.cardRecentError) {
      els.cardModalContent.innerHTML = `
        <div class="alert alert--warn">
          <div>Couldn’t load recent charges.</div>
          <button class="btn btn--secondary" type="button" id="cashBillsRetryCardRecent">${COPY.error.retry}</button>
        </div>
      `;
      const retry = document.getElementById("cashBillsRetryCardRecent");
      if (retry) retry.addEventListener("click", () => loadCardRecent());
      return;
    }
    const filtered = state.cardRecentCharges
      .map((item, idx) => ({ item, idx }))
      .filter(({ item }) => matchesMemberFilter(item) && matchesAccountFilter(item) && matchesApplePayFilter(item));
    if (!filtered.length) {
      if (state.cardRecentAppleOnly) {
        els.cardModalContent.innerHTML = `
          <div class="ui-muted">No Apple Pay transactions found in the last 30 days.</div>
          <div class="ui-muted cashbills-modal__hint">Click Apple Pay (30d) again to show all recent transactions.</div>
        `;
        return;
      }
      els.cardModalContent.innerHTML = `
        <div class="ui-muted">${COPY.cardRecurring.recentEmpty}</div>
        <div class="ui-muted cashbills-modal__hint">${COPY.cardRecurring.recentHelper}</div>
      `;
      return;
    }
    const filterNote = state.cardRecentAppleOnly
      ? `<div class="ui-muted" style="margin-bottom:8px">Filtered to Apple Pay transactions.</div>`
      : "";
    const rows = filtered
      .map(({ item, idx }) => {
        const isEditing = state.cardModalEdit && state.cardModalEdit.kind === "recent" && state.cardModalEdit.index === idx;
        const posted = item.posted_date ? formatDate(item.posted_date) : "—";
        const amount = item.amount != null ? currency.format(item.amount) : "—";
        const merchantLabel = formatMerchantLabel(item.merchant_display, item.description_sample, item.name);
        const descLabel = item.description_sample || "—";
        const cardLabel = formatCardLabel(item.source_account_name, item.source_account_mask);
        const cardIdLabel = formatPlaidAccountId(item.plaid_account_id);
        const cardCell = cardLabel || cardIdLabel ? `<div>${cardLabel || "—"}</div>${cardIdLabel ? `<div class="cashbills-modal__cell-muted">Acct ${cardIdLabel}</div>` : ""}` : "—";
        const memberCell = formatMemberCell(item.cardholder_name, item.cardholder_source);
        const dueDefault = item.posted_date ? String(new Date(item.posted_date).getDate()) : "";
        const editForm = isEditing
          ? `
            <div class="cashbills-inline-form" data-edit-kind="card-recent" data-index="${idx}">
              <label>Charge name<input class="bill-name" value="${merchantLabel}" /></label>
              <label>Charge day<input type="number" min="1" max="31" class="bill-day" value="${dueDefault}" /></label>
              <label>Amount mode
                <select class="bill-mode">
                  <option value="FIXED" selected>Fixed</option>
                  <option value="RANGE">Range</option>
                  <option value="VARIABLE">Variable</option>
                </select>
              </label>
              <label>Expected<input type="number" step="0.01" class="bill-expected" value="${item.amount || ""}" /></label>
              <label>Min<input type="number" step="0.01" class="bill-min" value="" /></label>
              <label>Max<input type="number" step="0.01" class="bill-max" value="" /></label>
              <div class="cashbills-inline-actions" style="grid-column: 1 / -1;">
                <button class="btn btn--primary js-card-recent-save" data-index="${idx}">Save</button>
                <button class="btn btn--secondary js-card-cancel" data-index="${idx}">Cancel</button>
              </div>
            </div>
          `
          : "";
        return `
          <div class="cashbills-modal__row">
            <div class="cashbills-modal__grid cashbills-modal__grid--card-recent">
              <div>${posted}</div>
              <div class="cashbills-modal__cell-wrap">${merchantLabel}</div>
              <div class="cashbills-modal__cell-wrap">${descLabel}</div>
              <div>${amount}</div>
              <div class="cashbills-modal__cell-wrap">${cardCell}</div>
              <div class="cashbills-modal__cell-wrap">${memberCell}</div>
              <div class="cashbills-modal__actions-row">
                <button class="btn btn--secondary js-card-recent-add" data-index="${idx}">Add</button>
              </div>
            </div>
            ${editForm}
          </div>
        `;
      })
      .join("");
    els.cardModalContent.innerHTML = `
      ${filterNote}
      <div class="cashbills-modal__table">
        <div class="cashbills-modal__grid cashbills-modal__grid--card-recent cashbills-modal__cell-muted">
          <div>Date</div>
          <div>Merchant</div>
          <div>Description</div>
          <div>Amount</div>
          <div>Card</div>
          <div>Member</div>
          <div></div>
        </div>
        ${rows}
      </div>
    `;
  };

  const renderCardModalFilters = () => {
    if (!els.cardModalFilters) return;
    const source =
      state.cardModalTab === "suggested"
        ? state.cardSuggestions
        : state.cardModalTab === "active"
          ? state.cardRecurringAllCharges
          : state.cardRecentCharges;
    const { options: memberOptions, hasUnknown } = collectMemberOptions(source);
    const { options: accountOptions, hasUnknown: hasUnknownAccount } = collectAccountOptions(source);
    if (!memberOptions.length && !hasUnknown) {
      state.cardMemberFilter = "ALL";
    }
    if (!accountOptions.length && !hasUnknownAccount) {
      state.cardAccountFilter = "ALL";
    }
    const applePayOnly = state.cardModalTab === "recent" && state.cardRecentAppleOnly;
    const filtered =
      state.cardModalTab === "active"
        ? applyCardFilters(source)
        : source.filter((row) => matchesMemberFilter(row) && matchesAccountFilter(row) && (!applePayOnly || isApplePayCharge(row)));
    const total = filtered.reduce((sum, item) => {
      if (state.cardModalTab === "recent") {
        return sum + Math.abs(Number(item.amount || 0));
      }
      return sum + expectedChargeAmount(item);
    }, 0);
    const totalLabel =
      state.cardModalTab === "recent"
        ? applePayOnly
          ? "Apple Pay total (30d)"
          : "Total spent (30d)"
        : state.cardModalTab === "active"
          ? "Expected monthly total"
          : "Estimated monthly total";
    const filterParts = [];
    const memberSelect = [
      `<option value="ALL">All members</option>`,
      ...(hasUnknown ? [`<option value="UNKNOWN">Unknown</option>`] : []),
      ...memberOptions.map((option) => `<option value="${option.key}">${option.label}</option>`),
    ].join("");
    if (memberOptions.length || hasUnknown) {
      filterParts.push(`
        <label class="cashbills-modal__filter">
          Member
          <select id="cashBillsCardMemberFilter">${memberSelect}</select>
        </label>
      `);
    }
    const accountSelect = [
      `<option value="ALL">All accounts</option>`,
      ...(hasUnknownAccount ? [`<option value="UNKNOWN">Unknown</option>`] : []),
      ...accountOptions.map((option) => `<option value="${option.key}">${option.label}</option>`),
    ].join("");
    if (accountOptions.length || hasUnknownAccount) {
      filterParts.push(`
        <label class="cashbills-modal__filter">
          Card account
          <select id="cashBillsCardAccountFilter">${accountSelect}</select>
        </label>
      `);
    }
    filterParts.push(`
      <div class="cashbills-modal__filter-summary">
        <span class="ui-muted">${totalLabel}</span>
        <span class="ui-tabular-nums">${currency.format(total)}</span>
      </div>
    `);
    els.cardModalFilters.innerHTML = filterParts.join("");
    const memberSelectEl = document.getElementById("cashBillsCardMemberFilter");
    if (memberSelectEl) {
      const validKeys = new Set(["ALL", "UNKNOWN", ...memberOptions.map((option) => option.key)]);
      if (!validKeys.has(state.cardMemberFilter)) {
        state.cardMemberFilter = "ALL";
      }
      memberSelectEl.value = state.cardMemberFilter;
      memberSelectEl.addEventListener("change", (event) => {
        state.cardMemberFilter = event.target.value || "ALL";
        renderCardModal();
      });
    }
    const accountSelectEl = document.getElementById("cashBillsCardAccountFilter");
    if (accountSelectEl) {
      const validKeys = new Set(["ALL", "UNKNOWN", ...accountOptions.map((option) => option.key)]);
      if (!validKeys.has(state.cardAccountFilter)) {
        state.cardAccountFilter = "ALL";
      }
      accountSelectEl.value = state.cardAccountFilter;
      accountSelectEl.addEventListener("change", (event) => {
        state.cardAccountFilter = event.target.value || "ALL";
        renderCardModal();
      });
    }
  };

  const renderCardModal = () => {
    if (!els.cardModal) return;
    els.cardModal.classList.toggle("is-open", state.cardModalOpen);
    els.cardModal.setAttribute("aria-hidden", state.cardModalOpen ? "false" : "true");
    if (!state.cardModalOpen) return;
    if (els.cardModalTabs) {
      Array.from(els.cardModalTabs.querySelectorAll("button")).forEach((btn) => {
        const isActive = btn.getAttribute("data-tab") === state.cardModalTab;
        btn.classList.toggle("ui-tab--active", isActive);
        btn.setAttribute("aria-pressed", isActive ? "true" : "false");
      });
    }
    if (els.cardModalApplePay) {
      const isActive = state.cardRecentAppleOnly;
      els.cardModalApplePay.classList.toggle("is-active", isActive);
      els.cardModalApplePay.setAttribute("aria-pressed", isActive ? "true" : "false");
    }
    if (state.cardModalTab === "suggested") {
      renderCardModalSuggestions();
    } else if (state.cardModalTab === "active") {
      renderCardModalActive();
    } else {
      renderCardModalRecent();
    }
    renderCardModalFilters();
    if (state.cardModalFocusId && state.cardModalTab === "active") {
      const focusId = state.cardModalFocusId;
      setTimeout(() => {
        const row = els.cardModalContent?.querySelector(`[data-charge-id="${focusId}"]`);
        if (!row) return;
        row.classList.add("is-focus");
        row.scrollIntoView({ block: "center", behavior: "smooth" });
        setTimeout(() => row.classList.remove("is-focus"), 2000);
        state.cardModalFocusId = null;
      }, 50);
    }
  };

  const openCardModal = () => {
    state.cardModalOpen = true;
    state.cardModalTab = "suggested";
    state.cardModalEdit = null;
    state.cardModalFocusId = null;
    state.cardRecentAppleOnly = false;
    renderCardModal();
    loadCardSuggestions();
  };

  const openCardModalWithFocus = (chargeId) => {
    state.cardModalOpen = true;
    state.cardModalTab = "active";
    state.cardModalEdit = { kind: "active", chargeId };
    state.cardModalFocusId = chargeId;
    state.cardRecentAppleOnly = false;
    renderCardModal();
    loadCardRecurringAll();
  };

  const closeCardModal = () => {
    state.cardModalOpen = false;
    state.cardModalEdit = null;
    state.cardModalFocusId = null;
    state.cardRecentAppleOnly = false;
    renderCardModal();
  };

  const toggleCardRecentApplePay = () => {
    state.cardModalOpen = true;
    state.cardModalTab = "recent";
    state.cardModalEdit = null;
    state.cardModalFocusId = null;
    state.cardRecentAppleOnly = !state.cardRecentAppleOnly;
    renderCardModal();
    loadCardRecent();
  };

  const openModal = () => {
    state.modalOpen = true;
    state.modalTab = "suggested";
    state.modalEdit = null;
    state.modalFocusBillId = null;
    renderModal();
    loadSuggestions();
  };

  const openModalWithFocus = (billId) => {
    state.modalOpen = true;
    state.modalTab = "active";
    state.modalEdit = { kind: "active", billId };
    state.modalFocusBillId = billId;
    renderModal();
    loadRecurringAll();
  };

  const closeModal = () => {
    state.modalOpen = false;
    state.modalEdit = null;
    state.modalFocusBillId = null;
    renderModal();
  };

  const updateRange = (range) => {
    state.rangeDays = Number(range);
    setChipActive(els.rangeChips, "data-range", range);
    render();
    refreshRecurringSummary();
  };

  const loadData = () => {
    state.loading = true;
    state.error = null;
    state.billsError = null;
    state.billsWarning = null;
    state.cashError = null;
    setLoading();
    useDashboardData()
      .then((data) => {
        state.data = data;
        state.billsError = data && data.bills_error ? String(data.bills_error) : null;
        state.billsWarning = data && data.bills_warning ? String(data.bills_warning) : null;
        state.cashError = data && data.cash_error ? String(data.cash_error) : null;
        state.loading = false;
        render();
      })
      .catch((err) => {
        state.loading = false;
        state.error = err ? String(err) : "Failed to load data";
        setError(state.error);
      });
    refreshRecurringSummary();
    refreshDepositSummary();
  };

  const setChipActive = (group, attr, value) => {
    if (!group) return;
    Array.from(group.querySelectorAll("button")).forEach((btn) => {
      const isActive = btn.getAttribute(attr) === value;
      btn.classList.toggle("is-active", isActive);
      btn.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  };

  const wireControls = () => {
    if (els.asOfSelect && els.customWrap && els.customDate) {
      els.customWrap.style.display = "none";
      els.asOfSelect.addEventListener("change", (e) => {
        const value = e.target.value;
        state.asOfMode = value;
        if (value === "custom") {
          els.customWrap.style.display = "grid";
          if (!els.customDate.value) {
            els.customDate.value = state.asOfDate;
          }
        } else {
          els.customWrap.style.display = "none";
          const base = new Date();
          if (value === "yesterday") base.setDate(base.getDate() - 1);
          state.asOfDate = toIsoDate(base);
          state.calendarMonthOffset = 0;
          state.suggestions = [];
          state.recurringAllBills = [];
          state.recentCharges = [];
          state.cardSuggestions = [];
          state.cardRecurringAllCharges = [];
          state.cardRecentCharges = [];
          state.depositsAccountId = null;
          state.depositsAccounts = [];
          state.depositsMonthly = [];
          render();
          refreshRecurringSummary();
          refreshDepositSummary(true);
        }
      });
      els.customDate.addEventListener("change", (e) => {
        if (e.target.value) {
          state.asOfDate = e.target.value;
          state.calendarMonthOffset = 0;
          state.suggestions = [];
          state.recurringAllBills = [];
          state.recentCharges = [];
          state.cardSuggestions = [];
          state.cardRecurringAllCharges = [];
          state.cardRecentCharges = [];
          state.depositsAccountId = null;
          state.depositsAccounts = [];
          state.depositsMonthly = [];
          render();
          refreshRecurringSummary();
          refreshDepositSummary(true);
        }
      });
    }
    if (els.scopeSelect) {
      els.scopeSelect.addEventListener("change", (e) => {
        state.scope = e.target.value;
        state.suggestions = [];
        state.recurringAllBills = [];
        state.recentCharges = [];
        state.cardSuggestions = [];
        state.cardRecurringAllCharges = [];
        state.cardRecentCharges = [];
        state.depositsAccountId = null;
        state.depositsAccounts = [];
        state.depositsMonthly = [];
        render();
        refreshRecurringSummary();
        refreshDepositSummary(true);
      });
    }
    if (els.refreshBtn) {
      els.refreshBtn.addEventListener("click", () => loadData());
    }
    if (els.rangeChips) {
      els.rangeChips.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn) return;
        const range = btn.getAttribute("data-range");
        if (!range) return;
        updateRange(range);
      });
    }
    if (els.statusChips) {
      els.statusChips.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn) return;
        const status = btn.getAttribute("data-status");
        if (!status) return;
        state.status = status;
        setChipActive(els.statusChips, "data-status", status);
        render();
      });
    }
    if (els.monthlyTotals) {
      els.monthlyTotals.addEventListener("change", (e) => {
        if (!e.target || e.target.id !== "cashBillsDepositAccount") return;
        state.depositsAccountId = e.target.value || null;
        refreshDepositSummary(true);
      });
    }
    if (els.recurringTable) {
      els.recurringTable.addEventListener("click", (e) => {
        const btn = e.target.closest(".js-set-due");
        if (!btn) return;
        const billId = Number(btn.getAttribute("data-bill-id"));
        if (!Number.isNaN(billId)) {
          openModalWithFocus(billId);
        }
      });
    }
    if (els.cardRecurringTable) {
      els.cardRecurringTable.addEventListener("click", (e) => {
        const btn = e.target.closest(".js-set-card-due");
        if (!btn) return;
        const chargeId = Number(btn.getAttribute("data-charge-id"));
        if (!Number.isNaN(chargeId)) {
          openCardModalWithFocus(chargeId);
        }
      });
    }
    if (els.billsTable) {
      els.billsTable.addEventListener("click", (e) => {
        const btn = e.target.closest(".js-payover");
        if (!btn || !state.data) return;
        const billId = btn.getAttribute("data-bill-id");
        if (!billId) return;
        const bill = (state.data.bills || []).find((row) => String(row.id) === billId);
        if (bill) {
          openPayOverModal(bill);
        }
      });
    }
    if (els.manageBillsBtn) {
      els.manageBillsBtn.addEventListener("click", () => openModal());
    }
    if (els.manageCardChargesBtn) {
      els.manageCardChargesBtn.addEventListener("click", () => openCardModal());
    }
    if (els.modalClose) {
      els.modalClose.addEventListener("click", () => closeModal());
    }
    if (els.modalRescan) {
      els.modalRescan.addEventListener("click", () => loadSuggestions(true));
    }
    if (els.cardModalClose) {
      els.cardModalClose.addEventListener("click", () => closeCardModal());
    }
    if (els.cardModalRescan) {
      els.cardModalRescan.addEventListener("click", () => loadCardSuggestions(true));
    }
    if (els.cardModalApplePay) {
      els.cardModalApplePay.addEventListener("click", () => toggleCardRecentApplePay());
    }
    if (els.financeModalClose) {
      els.financeModalClose.addEventListener("click", () => closeFinanceModal());
    }
    if (els.depositsModalClose) {
      els.depositsModalClose.addEventListener("click", () => closeDepositsModal());
    }
    if (els.payOverModalClose) {
      els.payOverModalClose.addEventListener("click", () => closePayOverModal());
    }
    if (els.calendarModalClose) {
      els.calendarModalClose.addEventListener("click", () => closeCalendarModal());
    }
    if (els.modal) {
      els.modal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closeModal();
        }
      });
    }
    if (els.cardModal) {
      els.cardModal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closeCardModal();
        }
      });
    }
    if (els.financeModal) {
      els.financeModal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closeFinanceModal();
        }
      });
    }
    if (els.depositsModal) {
      els.depositsModal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closeDepositsModal();
        }
      });
    }
    if (els.payOverModal) {
      els.payOverModal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closePayOverModal();
        }
      });
    }
    if (els.calendarModal) {
      els.calendarModal.addEventListener("click", (e) => {
        if (e.target && e.target.getAttribute("data-modal-close")) {
          closeCalendarModal();
        }
      });
    }
    if (els.modalTabs) {
      els.modalTabs.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn) return;
        const tab = btn.getAttribute("data-tab");
        if (!tab) return;
        state.modalTab = tab;
        state.modalEdit = null;
        renderModal();
        if (tab === "suggested") {
          loadSuggestions();
        } else if (tab === "active") {
          loadRecurringAll();
        } else {
          loadRecent();
        }
      });
    }
    if (els.cardModalTabs) {
      els.cardModalTabs.addEventListener("click", (e) => {
        const btn = e.target.closest("button");
        if (!btn) return;
        const tab = btn.getAttribute("data-tab");
        if (!tab) return;
        state.cardModalTab = tab;
        state.cardModalEdit = null;
        if (tab !== "recent") {
          state.cardRecentAppleOnly = false;
        }
        renderCardModal();
        if (tab === "suggested") {
          loadCardSuggestions();
        } else if (tab === "active") {
          loadCardRecurringAll();
        } else {
          loadCardRecent();
        }
      });
    }
    if (els.modalContent) {
      els.modalContent.addEventListener("click", (e) => {
        const addBtn = e.target.closest(".js-suggest-add");
        const recentAddBtn = e.target.closest(".js-recent-add");
        const ignoreBtn = e.target.closest(".js-suggest-ignore");
        const editBtn = e.target.closest(".js-bill-edit");
        const cancelBtn = e.target.closest(".js-bill-cancel");
        const saveBtn = e.target.closest(".js-bill-save");
        const recentSaveBtn = e.target.closest(".js-recent-save");
        const updateBtn = e.target.closest(".js-bill-update");
        if (addBtn) {
          const idx = Number(addBtn.getAttribute("data-index"));
          state.modalEdit = { kind: "suggestion", index: idx };
          renderModal();
          return;
        }
        if (recentAddBtn) {
          const idx = Number(recentAddBtn.getAttribute("data-index"));
          state.modalEdit = { kind: "recent", index: idx };
          renderModal();
          return;
        }
        if (editBtn) {
          const billId = Number(editBtn.getAttribute("data-bill-id"));
          state.modalEdit = { kind: "active", billId };
          renderModal();
          return;
        }
        if (cancelBtn) {
          state.modalEdit = null;
          renderModal();
          return;
        }
        if (ignoreBtn) {
          const idx = Number(ignoreBtn.getAttribute("data-index"));
          if (!ignoreBtn.dataset.confirm) {
            ignoreBtn.dataset.confirm = "1";
            ignoreBtn.textContent = "Confirm";
            return;
          }
          const item = state.suggestions[idx];
          if (!item) return;
          ignoreSuggestion({ candidate_key: item.key, scope: state.scope })
            .then(() => {
              state.suggestions = state.suggestions.filter((_, i) => i !== idx);
              refreshRecurringSummary();
              renderModal();
            })
            .catch(() => renderModal());
          return;
        }
        if (saveBtn) {
          const idx = Number(saveBtn.getAttribute("data-index"));
          const item = state.suggestions[idx];
          if (!item) return;
          const form = saveBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            candidate_key: item.key,
            name: form.querySelector(".bill-name")?.value || item.name,
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || item.amount_mode || "VARIABLE",
            amount_expected: form.querySelector(".bill-expected")?.value || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
            source_account_id: item.source_account_id,
            autodetect_confidence: item.confidence,
            scope: state.scope,
          };
          activateSuggestion(payload)
            .then(() => {
              state.modalEdit = null;
              state.suggestions = state.suggestions.filter((_, i) => i !== idx);
              refreshRecurringSummary();
              loadRecurringAll();
              renderModal();
            })
            .catch(() => renderModal());
          return;
        }
        if (recentSaveBtn) {
          const idx = Number(recentSaveBtn.getAttribute("data-index"));
          const item = state.recentCharges[idx];
          if (!item) return;
          const form = recentSaveBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            rule_type: item.rule_type,
            rule_value: item.rule_value,
            name: form.querySelector(".bill-name")?.value || item.merchant_display || "Monthly bill",
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || "FIXED",
            amount_expected: form.querySelector(".bill-expected")?.value || item.amount || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
            source_account_id: item.source_account_id,
            scope: state.scope,
          };
          activateSuggestion(payload)
            .then(() => {
              state.modalEdit = null;
              state.recentCharges = state.recentCharges.filter((_, i) => i !== idx);
              refreshRecurringSummary();
              loadRecurringAll();
              renderModal();
            })
            .catch(() => renderModal());
          return;
        }
        if (updateBtn) {
          const billId = Number(updateBtn.getAttribute("data-bill-id"));
          const form = updateBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            name: form.querySelector(".bill-name")?.value || "",
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || "VARIABLE",
            amount_expected: form.querySelector(".bill-expected")?.value || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
          };
          updateBill(billId, payload)
            .then(() => {
              state.modalEdit = null;
              refreshRecurringSummary();
              loadRecurringAll();
            })
            .catch(() => renderModal());
          return;
        }
      });
      els.modalContent.addEventListener("change", (e) => {
        const toggle = e.target.closest(".js-bill-toggle");
        if (toggle) {
          const billId = Number(toggle.getAttribute("data-bill-id"));
          updateBill(billId, { is_active: toggle.checked })
            .then(() => {
              refreshRecurringSummary();
              loadRecurringAll();
            })
            .catch(() => renderModal());
        }
      });
    }
    if (els.cardModalContent) {
      els.cardModalContent.addEventListener("click", (e) => {
        const addBtn = e.target.closest(".js-card-add");
        const recentAddBtn = e.target.closest(".js-card-recent-add");
        const ignoreBtn = e.target.closest(".js-card-ignore");
        const editBtn = e.target.closest(".js-card-edit");
        const cancelBtn = e.target.closest(".js-card-cancel");
        const saveBtn = e.target.closest(".js-card-save");
        const recentSaveBtn = e.target.closest(".js-card-recent-save");
        const updateBtn = e.target.closest(".js-card-update");
        if (addBtn) {
          const idx = Number(addBtn.getAttribute("data-index"));
          state.cardModalEdit = { kind: "suggestion", index: idx };
          renderCardModal();
          return;
        }
        if (recentAddBtn) {
          const idx = Number(recentAddBtn.getAttribute("data-index"));
          state.cardModalEdit = { kind: "recent", index: idx };
          renderCardModal();
          return;
        }
        if (editBtn) {
          const chargeId = Number(editBtn.getAttribute("data-charge-id"));
          state.cardModalEdit = { kind: "active", chargeId };
          renderCardModal();
          return;
        }
        if (cancelBtn) {
          state.cardModalEdit = null;
          renderCardModal();
          return;
        }
        if (ignoreBtn) {
          const idx = Number(ignoreBtn.getAttribute("data-index"));
          if (!ignoreBtn.dataset.confirm) {
            ignoreBtn.dataset.confirm = "1";
            ignoreBtn.textContent = "Confirm";
            return;
          }
          const item = state.cardSuggestions[idx];
          if (!item) return;
          ignoreCardSuggestion({ candidate_key: item.key, scope: state.scope })
            .then(() => {
              state.cardSuggestions = state.cardSuggestions.filter((_, i) => i !== idx);
              refreshCardRecurringSummary();
              renderCardModal();
            })
            .catch(() => renderCardModal());
          return;
        }
        if (saveBtn) {
          const idx = Number(saveBtn.getAttribute("data-index"));
          const item = state.cardSuggestions[idx];
          if (!item) return;
          const form = saveBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            candidate_key: item.key,
            name: form.querySelector(".bill-name")?.value || item.name,
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || item.amount_mode || "VARIABLE",
            amount_expected: form.querySelector(".bill-expected")?.value || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
            source_account_id: item.source_account_id,
            autodetect_confidence: item.confidence,
            scope: state.scope,
          };
          activateCardSuggestion(payload)
            .then(() => {
              state.cardModalEdit = null;
              state.cardSuggestions = state.cardSuggestions.filter((_, i) => i !== idx);
              refreshCardRecurringSummary();
              loadCardRecurringAll();
              renderCardModal();
            })
            .catch(() => renderCardModal());
          return;
        }
        if (recentSaveBtn) {
          const idx = Number(recentSaveBtn.getAttribute("data-index"));
          const item = state.cardRecentCharges[idx];
          if (!item) return;
          const form = recentSaveBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            rule_type: item.rule_type,
            rule_value: item.rule_value,
            name: form.querySelector(".bill-name")?.value || item.merchant_display || "Card charge",
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || "FIXED",
            amount_expected: form.querySelector(".bill-expected")?.value || item.amount || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
            source_account_id: item.source_account_id,
            scope: state.scope,
          };
          activateCardSuggestion(payload)
            .then(() => {
              state.cardModalEdit = null;
              state.cardRecentCharges = state.cardRecentCharges.filter((_, i) => i !== idx);
              refreshCardRecurringSummary();
              loadCardRecurringAll();
              renderCardModal();
            })
            .catch(() => renderCardModal());
          return;
        }
        if (updateBtn) {
          const chargeId = Number(updateBtn.getAttribute("data-charge-id"));
          const form = updateBtn.closest(".cashbills-inline-form");
          if (!form) return;
          const payload = {
            name: form.querySelector(".bill-name")?.value || "",
            due_day_of_month: form.querySelector(".bill-day")?.value || null,
            amount_mode: form.querySelector(".bill-mode")?.value || "VARIABLE",
            amount_expected: form.querySelector(".bill-expected")?.value || null,
            amount_min: form.querySelector(".bill-min")?.value || null,
            amount_max: form.querySelector(".bill-max")?.value || null,
          };
          updateCardCharge(chargeId, payload)
            .then(() => {
              state.cardModalEdit = null;
              refreshCardRecurringSummary();
              loadCardRecurringAll();
            })
            .catch(() => renderCardModal());
          return;
        }
      });
      els.cardModalContent.addEventListener("change", (e) => {
        const toggle = e.target.closest(".js-card-toggle");
        if (toggle) {
          const chargeId = Number(toggle.getAttribute("data-charge-id"));
          updateCardCharge(chargeId, { is_active: toggle.checked })
            .then(() => {
              refreshCardRecurringSummary();
              loadCardRecurringAll();
            })
            .catch(() => renderCardModal());
        }
      });
    }
    if (els.authBanner && els.dismissBanner) {
      const dismissed = sessionStorage.getItem("cashBillsAuthDismissed") === "1";
      if (dismissed) {
        els.authBanner.style.display = "none";
      }
      els.dismissBanner.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        sessionStorage.setItem("cashBillsAuthDismissed", "1");
        els.authBanner.style.display = "none";
      });
    }
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        if (state.modalOpen) closeModal();
        if (state.cardModalOpen) closeCardModal();
        if (state.financeModalOpen) closeFinanceModal();
        if (state.calendarModalOpen) closeCalendarModal();
        if (state.payOverModalOpen) closePayOverModal();
      }
    });
  };

  wireControls();
  loadData();
})();
