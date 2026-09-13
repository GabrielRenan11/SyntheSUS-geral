/* ============================================================
   SyntheSUS Dashboard
   Front-end estático consumindo a API FastAPI publicada no Render.
   ============================================================ */

const API_BASE = "https://synthesus-api.onrender.com";

const charts = {};
let loadingCount = 0;
let currentView = "geral";

const pageMeta = {
    geral: {
        title: "Visão geral",
        subtitle: "Panorama nacional de internações e infraestrutura hospitalar."
    },
    unidades: {
        title: "Unidades",
        subtitle: "Alvarás, tipos de unidade, demanda e infraestrutura por CNES."
    },
    leitos: {
        title: "Disponibilidade de leitos",
        subtitle: "Capacidade hospitalar cadastrada e disponibilidade ao SUS."
    },
    equipamentos: {
        title: "Disponibilidade de equipamentos",
        subtitle: "Cadastro, operação e disponibilidade de equipamentos para o SUS."
    },
    profissionais: {
        title: "Disponibilidade de profissionais",
        subtitle: "Profissionais cadastrados, vínculos SUS, carga horária e ocupações."
    },
    internacoes: {
        title: "Internações",
        subtitle: "Indicadores clínicos, custos, diagnósticos e perfil demográfico."
    }
};

const scopeConfig = {
    geral: {
        regiao: "geralRegiao", uf: "geralUf", municipio: "geralMunicipio"
    },
    unidades: {
        regiao: "unidadesRegiao", uf: "unidadesUf", municipio: "unidadesMunicipio", cnes: "unidadesCnes"
    },
    leitos: {
        regiao: "leitosRegiao", uf: "leitosUf", municipio: "leitosMunicipio", cnes: "leitosCnes"
    },
    equipamentos: {
        regiao: "equipRegiao", uf: "equipUf", municipio: "equipMunicipio", cnes: "equipCnes"
    },
    profissionais: {
        regiao: "profRegiao", uf: "profUf", municipio: "profMunicipio", cnes: "profCnes"
    },
    internacoes: {
        regiao: "intRegiao", uf: "intUf", municipio: "intMunicipio", cnes: "intCnes"
    }
};

const palette = [
    "#0f6aa6", "#1f9d76", "#2d8dc5", "#5fc7a2", "#5a83b8",
    "#6fbfcb", "#89b64b", "#9f83c7", "#d19c45", "#de6f70",
    "#7f93a0"
];

const numberFmt = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });
const decimalFmt = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
const currencyFmt = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

document.addEventListener("DOMContentLoaded", async () => {
    setDefaultDates();
    wireNavigation();
    wireFilters();
    wireActions();
    await loadRegionsIntoAllScopes();
    await checkApi();
    await refreshView("geral");
});

function el(id) {
    return document.getElementById(id);
}

function value(id) {
    return el(id)?.value?.trim() ?? "";
}

function showLoading() {
    loadingCount += 1;
    el("loadingOverlay").classList.add("show");
    el("loadingOverlay").setAttribute("aria-hidden", "false");
}

function hideLoading() {
    loadingCount = Math.max(0, loadingCount - 1);
    if (loadingCount === 0) {
        el("loadingOverlay").classList.remove("show");
        el("loadingOverlay").setAttribute("aria-hidden", "true");
    }
}

function toast(message) {
    el("toastBody").textContent = message;
    bootstrap.Toast.getOrCreateInstance(el("appToast"), { delay: 4200 }).show();
}

async function api(path, params = {}) {
    const url = new URL(`${API_BASE}${path}`);

    Object.entries(params).forEach(([key, val]) => {
        if (val !== undefined && val !== null && String(val).trim() !== "") {
            url.searchParams.set(key, val);
        }
    });

    const response = await fetch(url.toString(), {
        headers: { "Accept": "application/json" }
    });

    if (!response.ok) {
        let detail = `${response.status} ${response.statusText}`;
        try {
            const body = await response.json();
            detail = body.detail || body.erro || JSON.stringify(body);
        } catch (_) { }
        throw new Error(detail);
    }

    return response.json();
}

async function withLoading(task, errorPrefix = "Não foi possível carregar os dados") {
    showLoading();
    try {
        return await task();
    } catch (err) {
        console.error(err);
        toast(`${errorPrefix}: ${err.message}`);
        throw err;
    } finally {
        hideLoading();
    }
}

function setDefaultDates() {
    const now = new Date();
    const year = now.getFullYear();
    const yyyyMmDd = `${year}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    const yearStart = `${year}-01-01`;
    const currentMonth = `${year}-${String(now.getMonth() + 1).padStart(2, "0")}`;
    const monthStart = `${year}-01`;

    ["geralDataInicio", "intDataInicio"].forEach(id => el(id).value = yearStart);
    ["geralDataFim", "intDataFim"].forEach(id => el(id).value = yyyyMmDd);

    [
        "unidadesCompInicio", "leitosCompInicio", "equipCompInicio", "profCompInicio"
    ].forEach(id => el(id).value = monthStart);

    [
        "unidadesCompFim", "unidadesCompHall", "leitosCompFim",
        "equipCompFim", "profCompFim"
    ].forEach(id => el(id).value = currentMonth);
}

function wireNavigation() {
    document.querySelectorAll(".nav-item").forEach(btn => {
        btn.addEventListener("click", async () => {
            const view = btn.dataset.view;
            currentView = view;

            document.querySelectorAll(".nav-item").forEach(x => x.classList.toggle("active", x === btn));
            document.querySelectorAll(".dashboard-view").forEach(section => {
                section.classList.toggle("active", section.id === `view-${view}`);
            });

            el("pageTitle").textContent = pageMeta[view].title;
            el("pageSubtitle").textContent = pageMeta[view].subtitle;
            el("sidebar").classList.remove("open");

            await refreshView(view);
        });
    });

    el("sidebarToggle").addEventListener("click", () => el("sidebar").classList.toggle("open"));
    el("refreshCurrent").addEventListener("click", () => refreshView(currentView));
}

function wireFilters() {
    document.querySelectorAll(".geo-region").forEach(select => {
        select.addEventListener("change", async () => {
            const scope = select.dataset.scope;
            await loadUfs(scope);
            await loadMunicipios(scope);
            resetUnitSelect(scope);
        });
    });

    document.querySelectorAll(".geo-uf").forEach(select => {
        select.addEventListener("change", async () => {
            const scope = select.dataset.scope;
            await loadMunicipios(scope);
            resetUnitSelect(scope);
        });
    });

    document.querySelectorAll(".geo-municipio").forEach(select => {
        select.addEventListener("change", () => resetUnitSelect(select.dataset.scope));
    });

    document.querySelectorAll(".load-units").forEach(btn => {
        btn.addEventListener("click", () => loadUnits(btn.dataset.scope));
    });

    document.querySelectorAll("[data-clear]").forEach(btn => {
        btn.addEventListener("click", async () => {
            clearScope(btn.dataset.clear);
            await refreshView(btn.dataset.clear);
        });
    });

    el("unidadesComparativo").addEventListener("change", async () => {
        el("unidadesComparativoChip").textContent =
            `Comparativo: ${el("unidadesComparativo").selectedOptions[0].text.toLowerCase()}`;
        await refreshUnidadesEvolucao();
    });

    el("unidadesCnes").addEventListener("change", async () => {
        await Promise.allSettled([refreshAlvara(), refreshInfraestrutura(), refreshUnidadesEvolucao()]);
    });

    el("unidadesCompHall").addEventListener("change", refreshInfraestrutura);
}

function wireActions() {
    el("btnGeral").addEventListener("click", refreshGeral);
    el("btnUnidades").addEventListener("click", refreshUnidades);
    el("btnLeitos").addEventListener("click", refreshLeitos);
    el("btnEquipamentos").addEventListener("click", refreshEquipamentos);
    el("btnProfissionais").addEventListener("click", refreshProfissionais);
    el("btnInternacoes").addEventListener("click", refreshInternacoes);
}

function clearScope(scope) {
    const cfg = scopeConfig[scope];

    if (cfg) {
        [cfg.regiao, cfg.uf, cfg.municipio, cfg.cnes].filter(Boolean).forEach(id => {
            if (el(id)) el(id).value = "";
        });
    }

    if (scope === "equipamentos") el("equipTipo").value = "";
    if (scope === "internacoes") {
        ["intSexo", "intRaca", "intEtnia", "intCarater", "intIdadeMin", "intIdadeMax"].forEach(id => el(id).value = "");
    }

    setDefaultDates();
}

async function checkApi() {
    try {
        const data = await api("/health/database");
        const online = data.oracle === true;
        el("apiStatusDot").classList.toggle("online", online);
        el("apiStatusDot").classList.toggle("offline", !online);
        el("apiStatusText").textContent = online ? "API online" : "Banco indisponível";
    } catch (_) {
        el("apiStatusDot").classList.add("offline");
        el("apiStatusText").textContent = "API indisponível";
    }
}

function getGeo(scope) {
    const cfg = scopeConfig[scope];
    return {
        regiao: value(cfg.regiao),
        uf: value(cfg.uf),
        municipio: value(cfg.municipio)
    };
}

function getGeoAndUnit(scope) {
    return {
        ...getGeo(scope),
        cnes: scopeConfig[scope].cnes ? value(scopeConfig[scope].cnes) : ""
    };
}

async function loadRegionsIntoAllScopes() {
    try {
        const regions = await api("/regioes");
        document.querySelectorAll(".geo-region").forEach(select => {
            fillSelect(
                select,
                regions,
                item => item.regiao,
                item => item.regiao,
                "Todas"
            );
        });
    } catch (err) {
        toast(`Não foi possível carregar regiões: ${err.message}`);
    }
}

async function loadUfs(scope) {
    const cfg = scopeConfig[scope];
    const data = await api("/ufs", { regiao: value(cfg.regiao) });
    fillSelect(el(cfg.uf), data, item => item.uf, item => item.uf, "Todas");
}

async function loadMunicipios(scope) {
    const cfg = scopeConfig[scope];
    const data = await api("/municipios", {
        uf: value(cfg.uf),
        regiao: value(cfg.regiao)
    });

    fillSelect(
        el(cfg.municipio),
        data,
        item => item.id_municipio,
        item => `${item.nome_municipio} (${item.uf})`,
        "Todos"
    );
}

function fillSelect(select, items, valueFn, labelFn, emptyLabel) {
    const previous = select.value;
    select.innerHTML = `<option value="">${emptyLabel}</option>`;

    items.forEach(item => {
        const option = document.createElement("option");
        option.value = valueFn(item) ?? "";
        option.textContent = labelFn(item) ?? option.value;
        select.appendChild(option);
    });

    if ([...select.options].some(o => o.value === previous)) {
        select.value = previous;
    }
}

function resetUnitSelect(scope) {
    const id = scopeConfig[scope]?.cnes;
    if (!id) return;
    el(id).innerHTML = '<option value="">Todas as unidades</option>';
}

async function loadUnits(scope) {
    const cfg = scopeConfig[scope];
    if (!cfg?.cnes) return;

    const geo = getGeo(scope);
    if (!geo.uf && !geo.municipio) {
        toast("Selecione pelo menos uma UF ou município antes de carregar as unidades.");
        return;
    }

    await withLoading(async () => {
        const data = await api("/unidades", geo);
        fillSelect(
            el(cfg.cnes),
            data,
            item => item.cnes,
            item => `${item.nome_fantasia || item.razao_social || "Unidade"} — CNES ${item.cnes}`,
            "Todas as unidades"
        );
    }, "Não foi possível carregar as unidades");
}

function assertRange(start, end, label = "período") {
    if (!start || !end) {
        throw new Error(`Informe o ${label} completo.`);
    }
    if (start > end) {
        throw new Error("A data/competência inicial não pode ser posterior à final.");
    }
}

function formatNumber(v) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
    return numberFmt.format(Number(v));
}

function formatDecimal(v, suffix = "") {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
    return `${decimalFmt.format(Number(v))}${suffix}`;
}

function formatCurrency(v) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
    return currencyFmt.format(Number(v));
}

function formatDateBR(v) {
    if (!v) return "Não informada";
    const d = new Date(v);
    if (Number.isNaN(d.getTime())) return String(v);
    return d.toLocaleDateString("pt-BR", { timeZone: "UTC" });
}

function updateLastTime() {
    el("lastUpdate").textContent = `Atualizado às ${new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" })}`;
}

function destroyChart(id) {
    if (charts[id]) {
        charts[id].destroy();
        delete charts[id];
    }
}

function createChart(id, config) {
    destroyChart(id);
    const canvas = el(id);

    charts[id] = new Chart(canvas, {
        ...config,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 350 },
            plugins: {
                legend: {
                    display: true,
                    position: "bottom",
                    labels: {
                        usePointStyle: true,
                        boxWidth: 8,
                        color: "#5e707d",
                        font: { size: 10, weight: "600" }
                    }
                },
                tooltip: {
                    backgroundColor: "#0b2f47",
                    titleFont: { weight: "700" },
                    bodyFont: { size: 12 },
                    padding: 10,
                    cornerRadius: 9
                },
                ...(config.options?.plugins || {})
            },
            scales: config.type === "line" || config.type === "bar" ? {
                x: {
                    grid: { display: false },
                    ticks: { color: "#7b8b95", font: { size: 10 } }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: "rgba(120,145,160,.10)" },
                    ticks: { color: "#7b8b95", font: { size: 10 } }
                },
                ...(config.options?.scales || {})
            } : undefined,
            ...(config.options || {})
        }
    });
}

function pieChart(id, labels, values) {
    if (!values.length) {
        destroyChart(id);
        return;
    }

    createChart(id, {
        type: "doughnut",
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: labels.map((_, i) => palette[i % palette.length]),
                borderColor: "#ffffff",
                borderWidth: 2,
                hoverOffset: 6
            }]
        },
        options: {
            cutout: "58%",
            plugins: {
                legend: {
                    display: true,
                    position: "bottom",
                    labels: { usePointStyle: true, boxWidth: 7, font: { size: 9 }, color: "#5e707d" }
                }
            }
        }
    });
}

function lineChart(id, labels, datasets) {
    createChart(id, {
        type: "line",
        data: { labels, datasets },
        options: {
            interaction: { intersect: false, mode: "index" },
            elements: {
                line: { tension: .28, borderWidth: 2.2 },
                point: { radius: 2.8, hoverRadius: 5 }
            }
        }
    });
}

function barChart(id, labels, values, label) {
    createChart(id, {
        type: "bar",
        data: {
            labels,
            datasets: [{
                label,
                data: values,
                backgroundColor: "rgba(15,106,166,.72)",
                borderColor: "#0f6aa6",
                borderWidth: 1,
                borderRadius: 7,
                maxBarThickness: 46
            }]
        },
        options: {
            plugins: { legend: { display: false } }
        }
    });
}

function ranking(containerId, rows, labelKey, valueKey, valueFormatter = formatNumber) {
    const container = el(containerId);
    container.innerHTML = "";

    if (!rows?.length) {
        container.innerHTML = '<div class="empty-state compact"><i class="bi bi-inbox"></i><span>Nenhum dado encontrado para os filtros selecionados.</span></div>';
        return;
    }

    rows.slice(0, 10).forEach((row, index) => {
        const div = document.createElement("div");
        div.className = "rank-row";
        div.innerHTML = `
      <span class="rank-position">${index + 1}</span>
      <span class="rank-label" title="${escapeHtml(String(row[labelKey] ?? "Não informado"))}">${escapeHtml(String(row[labelKey] ?? "Não informado"))}</span>
      <span class="rank-value">${valueFormatter(row[valueKey])}</span>
    `;
        container.appendChild(div);
    });
}

function escapeHtml(str) {
    return str
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function topWithOthers(rows, labelKey, valueKey, limit = 10) {
    if (!rows?.length) return { labels: [], values: [] };

    const sorted = [...rows].sort((a, b) => Number(b[valueKey] || 0) - Number(a[valueKey] || 0));
    const head = sorted.slice(0, limit);
    const rest = sorted.slice(limit).reduce((sum, x) => sum + Number(x[valueKey] || 0), 0);

    const labels = head.map(x => String(x[labelKey] ?? "Não informado"));
    const values = head.map(x => Number(x[valueKey] || 0));

    if (rest > 0) {
        labels.push("Outros");
        values.push(rest);
    }

    return { labels, values };
}

async function refreshView(view) {
    const map = {
        geral: refreshGeral,
        unidades: refreshUnidades,
        leitos: refreshLeitos,
        equipamentos: refreshEquipamentos,
        profissionais: refreshProfissionais,
        internacoes: refreshInternacoes
    };

    await map[view]();
}

async function refreshGeral() {
    const start = value("geralDataInicio");
    const end = value("geralDataFim");

    try {
        assertRange(start, end);
        const params = { data_entrada: start, data_saida: end, ...getGeo("geral") };

        await withLoading(async () => {
            const [resumo, evolucao, hospitais, diagnosticos] = await Promise.all([
                api("/geral/resumo", params),
                api("/geral/internacoes-evolucao", params),
                api("/geral/hospitais-demanda", params),
                api("/geral/diagnosticos", params)
            ]);

            el("geralTotalInternacoes").textContent = formatNumber(resumo.total_internacoes);
            el("geralTotalLeitos").textContent = formatNumber(resumo.total_leitos);
            el("geralPermanenciaMedia").textContent = formatDecimal(resumo.permanencia_media, " dias");
            el("geralTotalUnidades").textContent = formatNumber(resumo.total_unidades);

            lineChart("chartGeralEvolucao",
                evolucao.map(x => x.competencia),
                [{
                    label: "Internações",
                    data: evolucao.map(x => x.internacoes),
                    borderColor: "#0f6aa6",
                    backgroundColor: "rgba(15,106,166,.12)",
                    fill: true
                }]
            );

            pieChart(
                "chartGeralHospitais",
                hospitais.map(x => x.unidade || x.cnes),
                hospitais.map(x => x.internacoes)
            );

            pieChart(
                "chartGeralDiagnosticos",
                diagnosticos.map(x => x.diagnostico),
                diagnosticos.map(x => x.internacoes)
            );

            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

async function refreshUnidades() {
    const compStart = value("unidadesCompInicio");
    const compEnd = value("unidadesCompFim");

    try {
        assertRange(compStart, compEnd, "período de competências");
        const geo = getGeo("unidades");
        const endDate = monthEndDate(compEnd);
        const startDate = `${compStart}-01`;

        await withLoading(async () => {
            const [alvaras, tipos, uti] = await Promise.all([
                api("/unidades/alvaras-resumo", { competencia_inicio: compStart, competencia_fim: compEnd, ...geo }),
                api("/unidades/tipos-ranking", { competencia: compEnd, ...geo }),
                api("/unidades/uti-ranking", { data_entrada: startDate, data_saida: endDate, ...geo })
            ]);

            el("unidadesComAlvara").textContent = formatNumber(alvaras.com_alvara);
            el("unidadesSemAlvara").textContent = formatNumber(alvaras.sem_alvara);
            el("unidadesTotal").textContent = formatNumber(alvaras.total_unidades);

            ranking("rankingTiposUnidade", tipos, "tipo_unidade", "unidades");

            barChart(
                "chartUnidadesUti",
                uti.map(x => x.cnes),
                uti.map(x => x.permanencia_media_uti),
                "Permanência média em UTI (dias)"
            );

            await Promise.allSettled([
                refreshUnidadesEvolucao(false),
                refreshAlvara(false),
                refreshInfraestrutura(false)
            ]);

            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

async function refreshUnidadesEvolucao(withSpinner = true) {
    const compStart = value("unidadesCompInicio");
    const compEnd = value("unidadesCompFim");
    if (!compStart || !compEnd) return;

    const run = async () => {
        const data = await api("/unidades/internacoes-evolucao", {
            data_entrada: `${compStart}-01`,
            data_saida: monthEndDate(compEnd),
            ...getGeoAndUnit("unidades")
        });

        const key = value("unidadesComparativo") || "media_por_unidade";
        const label = key === "mediana_por_unidade" ? "Mediana por unidade" : "Média por unidade";

        lineChart("chartUnidadesEvolucao",
            data.map(x => x.competencia),
            [
                {
                    label: "Total de internações",
                    data: data.map(x => x.total_internacoes),
                    borderColor: "#0f6aa6",
                    backgroundColor: "rgba(15,106,166,.08)"
                },
                {
                    label,
                    data: data.map(x => x[key]),
                    borderColor: "#1f9d76",
                    backgroundColor: "rgba(31,157,118,.08)"
                }
            ]
        );
    };

    if (withSpinner) return withLoading(run, "Erro ao carregar evolução das unidades");
    return run();
}

async function refreshAlvara(withSpinner = true) {
    const cnes = value("unidadesCnes");
    const card = el("unidadeAlvaraCard");

    if (!cnes) {
        card.innerHTML = '<div class="empty-state compact"><i class="bi bi-card-checklist"></i><span>Selecione uma unidade para consultar o alvará.</span></div>';
        return;
    }

    const run = async () => {
        const data = await api(`/unidades/${encodeURIComponent(cnes)}/alvara`);
        if (!data?.alvara) {
            card.innerHTML = '<div class="empty-state compact"><i class="bi bi-patch-exclamation"></i><span>Nenhum alvará cadastrado para esta unidade.</span></div>';
            return;
        }
        card.innerHTML = `
      <div class="permit-detail">
        <i class="bi bi-patch-check-fill"></i>
        <span class="eyebrow">CNES ${escapeHtml(cnes)}</span>
        <div class="permit-number">${escapeHtml(String(data.alvara))}</div>
        <div class="permit-date">Data de emissão: <strong>${escapeHtml(formatDateBR(data.data_emissao))}</strong></div>
      </div>
    `;
    };

    if (withSpinner) return withLoading(run, "Erro ao consultar alvará");
    return run();
}

async function refreshInfraestrutura(withSpinner = true) {
    const cnes = value("unidadesCnes");
    const competencia = value("unidadesCompHall");

    if (!cnes || !competencia) {
        el("infraHallBadge").textContent = "Selecione CNES + competência";
        ["chartInfraLeitos", "chartInfraEquipamentos", "chartInfraProfissionais"].forEach(destroyChart);
        return;
    }

    const run = async () => {
        const data = await api(`/unidades/${encodeURIComponent(cnes)}/infraestrutura`, { competencia });
        el("infraHallBadge").textContent = `CNES ${cnes} · ${competencia}`;

        const leitos = topWithOthers(data.leitos || [], "tipo_leito", "quantidade", 10);
        pieChart("chartInfraLeitos", leitos.labels, leitos.values);

        const equips = topWithOthers(data.equipamentos || [], "tipo_equipamento", "quantidade", 10);
        pieChart("chartInfraEquipamentos", equips.labels, equips.values);

        const profs = topWithOthers(data.profissionais || [], "cbo", "quantidade", 10);
        pieChart("chartInfraProfissionais", profs.labels, profs.values);
    };

    if (withSpinner) return withLoading(run, "Erro ao carregar infraestrutura");
    return run();
}

async function refreshLeitos() {
    const compStart = value("leitosCompInicio");
    const compEnd = value("leitosCompFim");

    try {
        assertRange(compStart, compEnd, "período de competências");
        const params = {
            competencia_inicio: compStart,
            competencia_fim: compEnd,
            ...getGeoAndUnit("leitos")
        };
        const geo = getGeo("leitos");

        await withLoading(async () => {
            const [resumo, unidades, rankUnidades, rankTipos] = await Promise.all([
                api("/leitos/resumo", params),
                api("/leitos/unidades", { competencia: compEnd, ...geo }),
                api("/leitos/ranking-unidades", { competencia: compEnd, ...geo }),
                api("/leitos/ranking-tipos", { competencia: compEnd, ...getGeoAndUnit("leitos") })
            ]);

            el("leitosExistentes").textContent = formatNumber(resumo.total_leitos_existentes);
            el("leitosSus").textContent = formatNumber(resumo.leitos_disponiveis_sus);
            el("leitosContratados").textContent = formatNumber(resumo.leitos_contratados);
            el("leitosUnidadesCount").textContent = formatNumber(resumo.unidades_com_leitos);

            renderUnitBedsList(unidades);

            pieChart(
                "chartLeitosUnidades",
                rankUnidades.map(x => `CNES ${x.cnes}`),
                rankUnidades.map(x => x.leitos_existentes)
            );

            pieChart(
                "chartLeitosTipos",
                rankTipos.map(x => x.tipo_leito),
                rankTipos.map(x => x.leitos_existentes)
            );

            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

function renderUnitBedsList(rows) {
    const container = el("listaUnidadesLeitos");
    container.innerHTML = "";

    if (!rows?.length) {
        container.innerHTML = '<div class="empty-state compact"><i class="bi bi-inbox"></i><span>Nenhuma unidade com leitos para este recorte.</span></div>';
        return;
    }

    rows.forEach(row => {
        const item = document.createElement("div");
        item.className = "data-list-item";
        item.innerHTML = `
      <div>
        <strong>CNES ${escapeHtml(String(row.cnes))}</strong>
        <small>${escapeHtml(String(row.nome_municipio || ""))}${row.uf ? ` · ${escapeHtml(String(row.uf))}` : ""}</small>
      </div>
      <span class="value">${formatNumber(row.leitos_existentes)} leitos</span>
    `;
        container.appendChild(item);
    });
}

async function refreshEquipamentos() {
    const compStart = value("equipCompInicio");
    const compEnd = value("equipCompFim");

    try {
        assertRange(compStart, compEnd, "período de competências");
        const common = {
            competencia_inicio: compStart,
            competencia_fim: compEnd,
            ...getGeoAndUnit("equipamentos"),
            tipo: value("equipTipo")
        };

        const rankingParams = {
            competencia: compEnd,
            ...getGeoAndUnit("equipamentos")
        };

        await withLoading(async () => {
            const [resumo, rankingData, evolucao] = await Promise.all([
                api("/equipamentos/resumo", common),
                api("/equipamentos/ranking-tipos", rankingParams),
                api("/equipamentos/evolucao", common)
            ]);

            el("equipTotal").textContent = formatNumber(resumo.total_equipamentos_cadastrados);
            el("equipOperacionais").textContent = formatNumber(resumo.total_equipamentos_operacionais);
            el("equipSus").textContent = formatNumber(resumo.total_equipamentos_sus);

            ranking("rankingEquipamentos", rankingData, "tipo_equipamento", "quantidade");
            fillEquipmentDatalist(rankingData);

            lineChart("chartEquipEvolucao",
                evolucao.map(x => x.competencia),
                [
                    { label: "Cadastrados", data: evolucao.map(x => x.media_cadastrados), borderColor: "#0f6aa6", backgroundColor: "rgba(15,106,166,.06)" },
                    { label: "Operacionais", data: evolucao.map(x => x.media_operacionais), borderColor: "#1f9d76", backgroundColor: "rgba(31,157,118,.06)" },
                    { label: "Disponíveis ao SUS", data: evolucao.map(x => x.media_disponiveis_sus), borderColor: "#9f83c7", backgroundColor: "rgba(159,131,199,.06)" }
                ]
            );

            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

function fillEquipmentDatalist(rows) {
    const datalist = el("equipTiposDatalist");
    datalist.innerHTML = "";
    rows.forEach(row => {
        const option = document.createElement("option");
        option.value = row.tipo_equipamento ?? "";
        datalist.appendChild(option);
    });
}

async function refreshProfissionais() {
    const compStart = value("profCompInicio");
    const compEnd = value("profCompFim");

    try {
        assertRange(compStart, compEnd, "período de competências");
        const base = {
            competencia_inicio: compStart,
            competencia_fim: compEnd,
            ...getGeoAndUnit("profissionais")
        };

        await withLoading(async () => {
            const [resumo, ocupacoes] = await Promise.all([
                api("/profissionais/resumo", base),
                api("/profissionais/ocupacoes", { competencia: compEnd, ...getGeoAndUnit("profissionais") })
            ]);

            el("profTotal").textContent = formatNumber(resumo.total_profissionais_cadastrados);
            el("profSus").textContent = formatNumber(resumo.total_profissionais_sus);
            el("profCarga").textContent = resumo.carga_horaria_disponivel
                ? formatDecimal(resumo.carga_horaria_media, " h")
                : "—";
            el("profMediaUnidade").textContent = formatDecimal(resumo.media_profissionais_por_unidade);

            ranking("rankingProfissionais", ocupacoes, "cbo", "profissionais");
            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

async function refreshInternacoes() {
    const start = value("intDataInicio");
    const end = value("intDataFim");

    try {
        assertRange(start, end);

        const common = {
            data_entrada: start,
            data_saida: end,
            ...getGeoAndUnit("internacoes"),
            sexo: value("intSexo"),
            raca_cor: value("intRaca"),
            etnia: value("intEtnia"),
            carater_internacao: value("intCarater"),
            idade_min: value("intIdadeMin"),
            idade_max: value("intIdadeMax")
        };

        const demographic = {
            data_entrada: start,
            data_saida: end,
            ...getGeoAndUnit("internacoes"),
            carater_internacao: value("intCarater")
        };

        await withLoading(async () => {
            const [resumo, diagnosticos, demografia] = await Promise.all([
                api("/internacoes/resumo", common),
                api("/internacoes/diagnosticos", common),
                api("/internacoes/demografia", demographic)
            ]);

            el("intTotal").textContent = formatNumber(resumo.total_internacoes);
            el("intPermanencia").textContent = formatDecimal(resumo.permanencia_media, " dias");
            el("intPermanenciaUti").textContent = formatDecimal(resumo.permanencia_media_uti, " dias");
            el("intValorMedio").textContent = formatCurrency(resumo.valor_medio_internacoes);

            ranking("rankingInternacoes", diagnosticos, "diagnostico", "internacoes");

            pieChart("chartIntSexo",
                (demografia.sexo || []).map(x => x.categoria),
                (demografia.sexo || []).map(x => x.quantidade)
            );

            pieChart("chartIntIdade",
                (demografia.idade || []).map(x => x.faixa),
                (demografia.idade || []).map(x => x.quantidade)
            );

            pieChart("chartIntRaca",
                (demografia.raca_cor || []).map(x => x.categoria),
                (demografia.raca_cor || []).map(x => x.quantidade)
            );

            pieChart("chartIntEtnia",
                (demografia.etnia || []).map(x => x.categoria),
                (demografia.etnia || []).map(x => x.quantidade)
            );

            updateLastTime();
        });
    } catch (err) {
        if (!String(err.message).includes("Não foi possível")) toast(err.message);
    }
}

function monthEndDate(yyyyMm) {
    const [year, month] = yyyyMm.split("-").map(Number);
    const lastDay = new Date(year, month, 0).getDate();
    return `${year}-${String(month).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;
}
