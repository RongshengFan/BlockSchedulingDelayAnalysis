const state = {
  meta: null,
  summary: null,
  report: null,
  gallery: null,
};

const SVG_NS = 'http://www.w3.org/2000/svg';
const CHART_COLORS = ['#2563eb', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];

function selectedValues(select) {
  return Array.from(select.selectedOptions).map(option => option.value);
}

function selectedSingleValue(select) {
  return select.value;
}

function metricLabel(metric) {
  return metric === 'sched' ? 'sched' : metric;
}

function titleCase(value) {
  return String(value).replace(/_/g, ' ');
}

function createSvg(width = 760, height = 360) {
  const svg = document.createElementNS(SVG_NS, 'svg');
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('class', 'plot-svg');
  return svg;
}

function svgEl(name, attrs = {}) {
  const element = document.createElementNS(SVG_NS, name);
  Object.entries(attrs).forEach(([key, value]) => element.setAttribute(key, String(value)));
  return element;
}

function clearPlot(targetId) {
  const target = document.getElementById(targetId);
  target.innerHTML = '';
  return target;
}

function triggerSvgDownload(targetId, filename) {
  const container = document.getElementById(targetId);
  const svg = container.querySelector('svg');
  if (!svg) {
    return;
  }
  const serializer = new XMLSerializer();
  const source = serializer.serializeToString(svg);
  const blob = new Blob([source], { type: 'image/svg+xml;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `${filename}.svg`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function renderEmptyState(targetId, message) {
  const target = clearPlot(targetId);
  const empty = document.createElement('div');
  empty.className = 'plot-empty';
  empty.textContent = message;
  target.appendChild(empty);
}

function appendCaption(target, text) {
  const caption = document.createElement('div');
  caption.className = 'plot-caption';
  caption.textContent = text;
  target.appendChild(caption);
}

function uniqueSortedNumbers(values) {
  return [...new Set(values.map(value => Number(value)).filter(value => Number.isFinite(value)))].sort((a, b) => a - b);
}

function formatShort(value) {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (abs >= 1_000) {
    return `${(value / 1_000).toFixed(1)}k`;
  }
  return `${Math.round(value * 100) / 100}`;
}

function drawAxes(svg, width, height, margin, yMax, xTicks, yTicks = 4) {
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  svg.appendChild(svgEl('line', { x1: margin.left, y1: margin.top, x2: margin.left, y2: margin.top + plotHeight, stroke: '#94a3b8', 'stroke-width': 1 }));
  svg.appendChild(svgEl('line', { x1: margin.left, y1: margin.top + plotHeight, x2: margin.left + plotWidth, y2: margin.top + plotHeight, stroke: '#94a3b8', 'stroke-width': 1 }));

  for (let i = 0; i <= yTicks; i += 1) {
    const ratio = i / yTicks;
    const y = margin.top + plotHeight - ratio * plotHeight;
    const value = yMax * ratio;
    svg.appendChild(svgEl('line', { x1: margin.left, y1: y, x2: margin.left + plotWidth, y2: y, stroke: 'rgba(148,163,184,0.25)', 'stroke-width': 1 }));
    const label = svgEl('text', { x: margin.left - 10, y: y + 4, 'text-anchor': 'end', 'font-size': 11, fill: '#64748b' });
    label.textContent = formatShort(value);
    svg.appendChild(label);
  }

  xTicks.forEach(({ x, label }) => {
    const tick = svgEl('text', { x, y: margin.top + plotHeight + 18, 'text-anchor': 'middle', 'font-size': 11, fill: '#64748b' });
    tick.textContent = String(label);
    svg.appendChild(tick);
  });
}

function drawXAxisLabel(svg, width, height, text) {
  const label = svgEl('text', { x: width / 2, y: height - 6, 'text-anchor': 'middle', 'font-size': 12, fill: '#64748b' });
  label.textContent = text;
  svg.appendChild(label);
}

function drawYAxisLabel(svg, height, text) {
  const label = svgEl('text', { x: 16, y: height / 2, transform: `rotate(-90 16 ${height / 2})`, 'text-anchor': 'middle', 'font-size': 12, fill: '#64748b' });
  label.textContent = text;
  svg.appendChild(label);
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`request failed: ${url}`);
  }
  return response.json();
}

function populateMultiSelect(element, values) {
  element.innerHTML = '';
  values.forEach(value => {
    const option = document.createElement('option');
    option.value = String(value);
    option.textContent = String(value);
    option.selected = true;
    element.appendChild(option);
  });
}

function populateSingleSelect(element, values) {
  element.innerHTML = '';
  values.forEach(value => {
    const option = document.createElement('option');
    option.value = String(value);
    option.textContent = String(value);
    element.appendChild(option);
  });
}

function buildQuery() {
  const params = new URLSearchParams();
  const workload = selectedSingleValue(document.getElementById('workloadFilter'));
  const batch = selectedSingleValue(document.getElementById('batchFilter'));
  if (workload && workload !== 'all') {
    params.append('workload', workload);
  }
  if (batch && batch !== 'all') {
    params.append('batch', batch);
  }
  return params.toString();
}

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

function renderStatus(validation) {
  const container = document.getElementById('validationStatus');
  const issueCounts = validation?.issue_counts || {};
  const batches = (validation?.batches || []).filter(value => Number(value) !== 8);
  container.innerHTML = '';
  [
    `Rows: ${validation?.rows ?? '-'}`,
    `Workloads: ${(validation?.workloads || []).join(', ') || '-'}`,
    `Batches: ${batches.join(', ') || '-'}`,
    `Errors: ${issueCounts.error ?? 0}`,
    `Warnings: ${issueCounts.warning ?? 0}`,
    `Passed: ${validation?.passed ? 'yes' : 'no'}`,
  ].forEach(line => {
    const div = document.createElement('div');
    div.textContent = line;
    container.appendChild(div);
  });
}

function renderInsights(report) {
  const top = report?.top_risk_workload || {};
  const low = report?.lowest_risk_workload || {};
  const schedFinding = (report?.sched_findings || []).find(item => Number(item?.batch) !== 8) || (report?.sched_findings || [])[0];
  const loadFinding = (report?.load_findings || []).find(item => Number(item?.batch) !== 8) || (report?.load_findings || [])[0];
  setText('topRiskSummary', top.workload ? `最高风险：${top.workload}（score=${Number(top.overall_risk_score ?? 0).toFixed(2)}）` : '-');
  setText('lowRiskSummary', low.workload ? `最低风险：${low.workload}（score=${Number(low.overall_risk_score ?? 0).toFixed(2)}）` : '-');
  setText('schedInsight', schedFinding?.detail || '-');
  setText('loadInsight', loadFinding?.detail || '-');
}

function renderCards(summary) {
  const ranking = summary.ranking || [];
  const sched = summary.sched || [];
  const load = summary.load || [];
  const validation = summary.validation || {};
  const topRisk = ranking[0];
  const peakSched = [...sched].sort((a, b) => (b.dispatch_gap_p95_cycles || 0) - (a.dispatch_gap_p95_cycles || 0))[0];
  const peakLoad = [...load].sort((a, b) => (b.block_imbalance_ratio || 0) - (a.block_imbalance_ratio || 0))[0];

  setText('topRiskWorkload', topRisk?.workload || '-');
  setText('topRiskScore', topRisk ? `score ${Number(topRisk.overall_risk_score).toFixed(2)}` : '-');
  setText('peakSchedP95', peakSched ? Number(peakSched.dispatch_gap_p95_cycles).toFixed(2) : '-');
  setText('peakSchedWorkload', peakSched ? `${peakSched.workload} / batch ${peakSched.batch}` : '-');
  setText('peakImbalance', peakLoad ? Number(peakLoad.block_imbalance_ratio).toFixed(4) : '-');
  setText('peakImbalanceWorkload', peakLoad ? `${peakLoad.workload} / batch ${peakLoad.batch}` : '-');
  setText('validationPassed', validation?.passed ? 'passed' : 'failed');
  setText('validationIssues', `issues ${validation?.total_issues ?? 0}`);
}

function renderLineChart(targetId, records, yField, title) {
  if (!records.length) {
    renderEmptyState(targetId, '当前筛选条件下没有数据');
    return;
  }
  const target = clearPlot(targetId);
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const tooltip = document.createElement('div');
  tooltip.className = 'plot-tooltip';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 20, right: 24, bottom: 42, left: 64 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xValues = uniqueSortedNumbers(records.map(item => item.batch));
  const yMax = Math.max(...records.map(item => Number(item[yField]) || 0), 1);
  const groups = new Map();
  const hidden = new Set();
  records.forEach(row => {
    if (!groups.has(row.workload)) {
      groups.set(row.workload, []);
    }
    groups.get(row.workload).push(row);
  });
  const xScale = value => {
    if (xValues.length <= 1) {
      return margin.left + plotWidth / 2;
    }
    return margin.left + ((value - xValues[0]) / (xValues[xValues.length - 1] - xValues[0])) * plotWidth;
  };
  const yScale = value => margin.top + plotHeight - (value / yMax) * plotHeight;

  drawAxes(svg, width, height, margin, yMax, xValues.map(value => ({ x: xScale(value), label: value })));

  const showTooltip = (event, lines) => {
    tooltip.innerHTML = lines.join('<br>');
    tooltip.style.display = 'block';
    tooltip.style.left = `${event.offsetX}px`;
    tooltip.style.top = `${event.offsetY}px`;
  };

  const hideTooltip = () => {
    tooltip.style.display = 'none';
  };

  Array.from(groups.entries()).forEach(([workload, values], index) => {
    const sorted = [...values].sort((a, b) => a.batch - b.batch);
    const points = sorted.map(item => `${xScale(Number(item.batch))},${yScale(Number(item[yField]) || 0)}`).join(' ');
    const color = CHART_COLORS[index % CHART_COLORS.length];
    const polyline = svgEl('polyline', { points, fill: 'none', stroke: color, 'stroke-width': 2.4, 'data-workload': workload });
    svg.appendChild(polyline);
    sorted.forEach(item => {
      const circle = svgEl('circle', {
        cx: xScale(Number(item.batch)),
        cy: yScale(Number(item[yField]) || 0),
        r: 3.8,
        fill: color,
        'data-workload': workload,
      });
      circle.addEventListener('mousemove', event => {
        showTooltip(event, [
          `<strong>${workload}</strong>`,
          `batch: ${item.batch}`,
          `${yField}: ${formatShort(Number(item[yField]) || 0)}`,
        ]);
      });
      circle.addEventListener('mouseleave', hideTooltip);
      svg.appendChild(circle);
    });
    const legendY = 18 + index * 16;
    const legendGroup = svgEl('g', { class: 'plot-legend', 'data-workload': workload });
    legendGroup.appendChild(svgEl('line', { x1: width - 120, y1: legendY, x2: width - 102, y2: legendY, stroke: color, 'stroke-width': 2.4 }));
    const legend = svgEl('text', { x: width - 96, y: legendY + 4, 'font-size': 11, fill: '#1e2430' });
    legend.textContent = workload;
    legendGroup.appendChild(legend);
    legendGroup.addEventListener('click', () => {
      if (hidden.has(workload)) {
        hidden.delete(workload);
      } else {
        hidden.add(workload);
      }
      svg.querySelectorAll(`[data-workload="${workload}"]`).forEach(node => {
        node.style.display = hidden.has(workload) ? 'none' : '';
      });
      legend.style.opacity = hidden.has(workload) ? '0.45' : '1';
    });
    svg.appendChild(legendGroup);
  });

  container.appendChild(tooltip);
  container.appendChild(svg);
  drawXAxisLabel(svg, width, height, 'batch');
  drawYAxisLabel(svg, height, titleCase(yField));
  appendCaption(container, title);
  target.appendChild(container);
}

function renderGroupedBarChart(targetId, records, yField, title) {
  if (!records.length) {
    renderEmptyState(targetId, '当前筛选条件下没有数据');
    return;
  }
  const target = clearPlot(targetId);
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 20, right: 24, bottom: 42, left: 64 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const batches = uniqueSortedNumbers(records.map(item => item.batch));
  const workloads = [...new Set(records.map(item => item.workload))];
  const yMax = Math.max(...records.map(item => Number(item[yField]) || 0), 1);
  const groupWidth = plotWidth / Math.max(batches.length, 1);
  const barWidth = Math.max(8, (groupWidth * 0.72) / Math.max(workloads.length, 1));

  drawAxes(svg, width, height, margin, yMax, batches.map((batch, index) => ({ x: margin.left + groupWidth * index + groupWidth / 2, label: batch })));
  const yScale = value => margin.top + plotHeight - (value / yMax) * plotHeight;

  batches.forEach((batch, batchIndex) => {
    workloads.forEach((workload, workloadIndex) => {
      const row = records.find(item => Number(item.batch) === batch && item.workload === workload);
      const value = Number(row?.[yField]) || 0;
      const x = margin.left + batchIndex * groupWidth + groupWidth * 0.14 + workloadIndex * barWidth;
      const y = yScale(value);
      svg.appendChild(svgEl('rect', {
        x,
        y,
        width: barWidth - 2,
        height: margin.top + plotHeight - y,
        rx: 6,
        fill: CHART_COLORS[workloadIndex % CHART_COLORS.length],
        opacity: 0.85,
      }));
    });
  });

  workloads.forEach((workload, index) => {
    const legendY = 18 + index * 16;
    const color = CHART_COLORS[index % CHART_COLORS.length];
    svg.appendChild(svgEl('rect', { x: width - 122, y: legendY - 8, width: 12, height: 12, rx: 3, fill: color }));
    const legend = svgEl('text', { x: width - 104, y: legendY + 2, 'font-size': 11, fill: '#1e2430' });
    legend.textContent = workload;
    svg.appendChild(legend);
  });

  container.appendChild(svg);
  drawXAxisLabel(svg, width, height, 'batch');
  drawYAxisLabel(svg, height, titleCase(yField));
  appendCaption(container, title);
  target.appendChild(container);
}

function renderRankingChart(records) {
  if (!records.length) {
    renderEmptyState('rankingChart', '没有 ranking 数据');
    return;
  }
  const target = clearPlot('rankingChart');
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 20, right: 24, bottom: 20, left: 110 };
  const plotWidth = width - margin.left - margin.right;
  const sorted = [...records].sort((a, b) => (b.overall_risk_score || 0) - (a.overall_risk_score || 0));
  const maxValue = Math.max(...sorted.map(item => Number(item.overall_risk_score) || 0), 1);
  const barHeight = 36;

  sorted.forEach((item, index) => {
    const y = margin.top + index * (barHeight + 12);
    const widthValue = ((Number(item.overall_risk_score) || 0) / maxValue) * plotWidth;
    svg.appendChild(svgEl('rect', { x: margin.left, y, width: widthValue, height: barHeight, rx: 10, fill: '#bf5b2c', opacity: 0.88 }));
    const name = svgEl('text', { x: margin.left - 10, y: y + 23, 'text-anchor': 'end', 'font-size': 12, fill: '#1e2430' });
    name.textContent = item.workload;
    svg.appendChild(name);
    const value = svgEl('text', { x: margin.left + widthValue + 8, y: y + 23, 'font-size': 12, fill: '#1e2430' });
    value.textContent = Number(item.overall_risk_score).toFixed(2);
    svg.appendChild(value);
  });

  container.appendChild(svg);
  appendCaption(container, 'Overall Risk Score');
  target.appendChild(container);
}

function renderScatterChart(targetId, records, xField, yField, sizeField, title) {
  if (!records.length) {
    renderEmptyState(targetId, '没有散点数据');
    return;
  }
  const target = clearPlot(targetId);
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 24, right: 24, bottom: 48, left: 68 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xMax = Math.max(...records.map(item => Number(item[xField]) || 0), 1);
  const yMax = Math.max(...records.map(item => Number(item[yField]) || 0), 1);
  const sizeMax = Math.max(...records.map(item => Number(item[sizeField]) || 0), 1);
  drawAxes(svg, width, height, margin, yMax, [{ x: margin.left, label: 0 }, { x: margin.left + plotWidth / 2, label: formatShort(xMax / 2) }, { x: margin.left + plotWidth, label: formatShort(xMax) }]);
  const xScale = value => margin.left + (value / xMax) * plotWidth;
  const yScale = value => margin.top + plotHeight - (value / yMax) * plotHeight;

  records.forEach((item, index) => {
    const radius = 8 + ((Number(item[sizeField]) || 0) / sizeMax) * 10;
    const color = CHART_COLORS[index % CHART_COLORS.length];
    svg.appendChild(svgEl('circle', { cx: xScale(Number(item[xField]) || 0), cy: yScale(Number(item[yField]) || 0), r: radius, fill: color, opacity: 0.55 }));
    const label = svgEl('text', { x: xScale(Number(item[xField]) || 0), y: yScale(Number(item[yField]) || 0) + 4, 'text-anchor': 'middle', 'font-size': 11, fill: '#1e2430' });
    label.textContent = item.workload;
    svg.appendChild(label);
  });

  container.appendChild(svg);
  drawXAxisLabel(svg, width, height, titleCase(xField));
  drawYAxisLabel(svg, height, titleCase(yField));
  appendCaption(container, title);
  target.appendChild(container);
}

function renderDistribution(records, metric) {
  if (!records.length) {
    renderEmptyState('distributionChart', '当前筛选条件下没有分布数据');
    return;
  }
  const groups = new Map();
  records.forEach(row => {
    const key = `${row.workload}-b${row.batch}`;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(row.value);
  });
  const target = clearPlot('distributionChart');
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 20, right: 24, bottom: 60, left: 64 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const entries = Array.from(groups.entries()).slice(0, 8);
  const values = entries.flatMap(([, groupValues]) => groupValues.map(Number).filter(Number.isFinite));
  const yMax = Math.max(...values, 1);
  drawAxes(svg, width, height, margin, yMax, entries.map(([,], index) => ({ x: margin.left + ((index + 0.5) / entries.length) * plotWidth, label: index + 1 })));

  const quantile = (array, q) => {
    const sorted = [...array].sort((a, b) => a - b);
    if (!sorted.length) {
      return 0;
    }
    const pos = (sorted.length - 1) * q;
    const base = Math.floor(pos);
    const rest = pos - base;
    return sorted[base + 1] !== undefined ? sorted[base] + rest * (sorted[base + 1] - sorted[base]) : sorted[base];
  };
  const yScale = value => margin.top + plotHeight - (value / yMax) * plotHeight;

  entries.forEach(([key, groupValues], index) => {
    const data = groupValues.map(Number).filter(Number.isFinite);
    const q1 = quantile(data, 0.25);
    const q2 = quantile(data, 0.50);
    const q3 = quantile(data, 0.75);
    const min = Math.min(...data);
    const max = Math.max(...data);
    const center = margin.left + ((index + 0.5) / entries.length) * plotWidth;
    const boxWidth = Math.min(42, plotWidth / entries.length / 1.8);
    const color = CHART_COLORS[index % CHART_COLORS.length];

    svg.appendChild(svgEl('line', { x1: center, y1: yScale(min), x2: center, y2: yScale(max), stroke: color, 'stroke-width': 1.4 }));
    svg.appendChild(svgEl('rect', { x: center - boxWidth / 2, y: yScale(q3), width: boxWidth, height: Math.max(2, yScale(q1) - yScale(q3)), fill: color, opacity: 0.3, stroke: color, 'stroke-width': 1.2 }));
    svg.appendChild(svgEl('line', { x1: center - boxWidth / 2, y1: yScale(q2), x2: center + boxWidth / 2, y2: yScale(q2), stroke: color, 'stroke-width': 1.6 }));

    const label = svgEl('text', { x: center, y: height - 18, 'text-anchor': 'middle', 'font-size': 10, fill: '#5b6474' });
    label.textContent = key;
    svg.appendChild(label);
  });

  container.appendChild(svg);
  appendCaption(container, `${metricLabel(metric)} distribution`);
  target.appendChild(container);
}

function renderHeatmap(records) {
  const workload = selectedSingleValue(document.getElementById('heatmapWorkload'));
  const batch = Number(selectedSingleValue(document.getElementById('heatmapBatch')));
  const metric = selectedSingleValue(document.getElementById('heatmapMetric'));
  const subset = records.filter(row => row.workload === workload && Number(row.batch) === batch);
  if (!subset.length) {
    renderEmptyState('heatmapChart', '没有 heatmap 数据');
    return;
  }
  const label = `${workload}-b${batch}`;
  const sorted = [...subset].sort((a, b) => a.sm - b.sm);
  const target = clearPlot('heatmapChart');
  const container = document.createElement('div');
  container.className = 'plot-shell';
  const svg = createSvg();
  const width = 760;
  const height = 360;
  const margin = { top: 30, right: 24, bottom: 52, left: 32 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = 120;
  const maxCount = Math.max(...sorted.map(item => Number(item[metric]) || 0), 1);
  const cellWidth = plotWidth / Math.max(sorted.length, 1);

  sorted.forEach((item, index) => {
    const intensity = (Number(item[metric]) || 0) / maxCount;
    const lightness = 92 - intensity * 46;
    svg.appendChild(svgEl('rect', {
      x: margin.left + index * cellWidth,
      y: margin.top + 40,
      width: Math.max(cellWidth - 1, 2),
      height: plotHeight,
      fill: `hsl(30 70% ${lightness}%)`,
      stroke: 'rgba(30,36,48,0.08)',
      'stroke-width': 0.5,
    }));
    if (index % 8 === 0) {
      const tick = svgEl('text', { x: margin.left + index * cellWidth + cellWidth / 2, y: margin.top + plotHeight + 58, 'text-anchor': 'middle', 'font-size': 10, fill: '#5b6474' });
      tick.textContent = String(item.sm);
      svg.appendChild(tick);
    }
  });

  const labelText = svgEl('text', { x: margin.left, y: 22, 'font-size': 12, fill: '#1e2430' });
  labelText.textContent = `${label} / ${metric}`;
  svg.appendChild(labelText);

  container.appendChild(svg);
  appendCaption(container, `SM ${metric} heatmap: ${label}`);
  target.appendChild(container);
}

function mergeSummary(summary) {
  const merged = new Map();
  const attach = (records, source) => {
    records.forEach(row => {
      const key = `${row.workload}::${row.batch}`;
      const existing = merged.get(key) || { workload: row.workload, batch: row.batch };
      Object.assign(existing, row);
      existing._source = [...new Set([...(existing._source || []), source])];
      merged.set(key, existing);
    });
  };
  attach(summary.sched || [], 'sched');
  attach(summary.load || [], 'load');
  return [...merged.values()].sort((a, b) => String(a.workload).localeCompare(String(b.workload)) || Number(a.batch) - Number(b.batch));
}

function renderTable(records) {
  const head = document.getElementById('summaryHead');
  const body = document.getElementById('summaryBody');
  head.innerHTML = '';
  body.innerHTML = '';
  if (!records.length) {
    return;
  }
  const columns = ['workload', 'batch', 'dispatch_gap_p95_cycles', 'sched_event_ratio', 'sched_cycles_per_sm_mean', 'block_imbalance_ratio', 'elapsed_sum_cv', 'jain_block_fairness'];
  const headerRow = document.createElement('tr');
  columns.forEach(column => {
    const th = document.createElement('th');
    th.textContent = column;
    headerRow.appendChild(th);
  });
  head.appendChild(headerRow);

  records.forEach(row => {
    const tr = document.createElement('tr');
    columns.forEach(column => {
      const td = document.createElement('td');
      let value = row[column];
      if (column === 'sched_event_ratio') {
        const numerator = Number(row.dispatch_gap_event_count) || 0;
        const denominator = Number(row.block_count_total) || 0;
        value = denominator > 0 ? numerator / denominator : 0;
      }
      td.textContent = value === undefined || value === null ? '-' : String(value);
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
}

function renderReport(report) {
  document.getElementById('reportView').textContent = JSON.stringify(report, null, 2);
}

function renderReportCards(report) {
  const container = document.getElementById('reportCards');
  container.innerHTML = '';
  const top = report?.top_risk_workload || {};
  const low = report?.lowest_risk_workload || {};
  const rankingMethod = report?.ranking_method || {};
  const pairwise = report?.correlation_summary?.pairwise || [];
  const corrPreview = pairwise
    .slice(0, 2)
    .map(item => {
      const spearman = item?.spearman;
      const value = Number.isFinite(spearman) ? spearman.toFixed(3) : 'NA';
      return `${item.label}：Spearman=${value}`;
    })
    .join('；');
  const cards = [
    {
      title: '综合排序',
      text: top.workload
        ? `首位：${top.workload}（score=${Number(top.overall_risk_score ?? 0).toFixed(2)}）；末位：${low.workload || '-'}（score=${Number(low.overall_risk_score ?? 0).toFixed(2)}）；方法：${rankingMethod.method || 'equal_weight_rank_sum'}`
        : '暂无综合排序信息。',
    },
    {
      title: '调度发现',
      text: (report?.sched_findings || []).map(item => item.detail).slice(0, 2).join('；') || '暂无调度发现。',
    },
    {
      title: '负载发现',
      text: (report?.load_findings || []).map(item => item.detail).slice(0, 2).join('；') || '暂无负载发现。',
    },
    {
      title: '相关性',
      text: corrPreview || '暂无相关性摘要。',
    },
    {
      title: '验证',
      text: report?.validation
        ? `passed=${report.validation.passed ? 'true' : 'false'}；issues=${report.validation.total_issues ?? 0}；rows=${report.validation.rows ?? '-'}`
        : '暂无验证信息。',
    },
  ];

  cards.forEach(card => {
    const item = document.createElement('article');
    item.className = 'report-card-item';
    item.innerHTML = `<strong>${card.title}</strong><p>${card.text}</p>`;
    container.appendChild(item);
  });
}

function renderGallery(groups) {
  const root = document.getElementById('galleryRoot');
  root.innerHTML = '';
  const total = groups.reduce((sum, group) => sum + group.count, 0);
  setText('galleryMeta', `${groups.length} categories / ${total} charts`);
  groups.forEach(group => {
    const section = document.createElement('section');
    section.className = 'gallery-group';
    const head = document.createElement('div');
    head.className = 'gallery-group-head';
    head.innerHTML = `<h3>${group.category}</h3><span class="mini-tag">${group.count} charts</span>`;
    section.appendChild(head);

    const grid = document.createElement('div');
    grid.className = 'gallery-grid';
    group.images.forEach(image => {
      const card = document.createElement('article');
      card.className = 'gallery-card';
      card.innerHTML = `
        <img src="${image.url}" alt="${image.title}" loading="lazy">
        <div class="gallery-card-body">
          <strong>${image.title}</strong>
          <div class="gallery-actions">
            <a class="gallery-link" href="${image.url}" target="_blank" rel="noreferrer">查看原图</a>
            <a class="gallery-link" href="${image.url}" download>下载 PNG</a>
          </div>
        </div>
      `;
      grid.appendChild(card);
    });
    section.appendChild(grid);
    root.appendChild(section);
  });
}

async function loadReport() {
  if (state.report) {
    renderReport(state.report);
    return;
  }
  document.getElementById('reportView').textContent = '正在加载结构化结论...';
  const reportData = await fetchJson('/api/report');
  state.report = reportData.report || {};
  renderReportCards(state.report);
  renderReport(state.report);
  renderInsights(state.report);
}

async function loadGallery() {
  if (state.gallery) {
    renderGallery(state.gallery);
    return;
  }
  const galleryData = await fetchJson('/api/gallery');
  state.gallery = galleryData.groups || [];
  renderGallery(state.gallery);
}

async function loadMeta() {
  const meta = await fetchJson('/api/meta');
  state.meta = meta;
  populateSingleSelect(document.getElementById('workloadFilter'), ['all', ...(meta.workloads || [])]);
  populateSingleSelect(document.getElementById('batchFilter'), ['all', ...(meta.batches || [])]);
  populateSingleSelect(document.getElementById('heatmapWorkload'), meta.workloads || []);
  populateSingleSelect(document.getElementById('heatmapBatch'), meta.batches || []);
  setText('metaInfo', meta.outputDir || '-');
}

async function refresh() {
  const query = buildQuery();
  const metric = 'sched';
  const [summary, distribution, heatmap] = await Promise.all([
    fetchJson(`/api/summary?${query}`),
    fetchJson(`/api/distribution?${query}&metric=${metric}`),
    fetchJson(`/api/heatmap?${query}`),
  ]);
  state.summary = summary;

  renderStatus(summary.validation || {});
  renderCards(summary);
  renderLineChart('schedChart', summary.sched || [], 'dispatch_gap_p95_cycles', 'Sched Gap P95 by Batch');
  const schedEventRatio = (summary.sched || []).map(row => {
    const numerator = Number(row.dispatch_gap_event_count) || 0;
    const denominator = Number(row.block_count_total) || 0;
    return { ...row, sched_event_ratio: denominator > 0 ? numerator / denominator : 0 };
  });
  renderLineChart('schedEventChart', schedEventRatio, 'sched_event_ratio', 'Sched Event Ratio by Batch');
  renderLineChart('loadChart', summary.load || [], 'block_imbalance_ratio', 'Load Imbalance by Batch');
  renderGroupedBarChart('fairnessChart', summary.load || [], 'jain_block_fairness', 'Jain Fairness by Batch');
  renderRankingChart(summary.ranking || []);
  renderScatterChart('riskScatterChart', summary.ranking || [], 'core_sched_p95_mean', 'imbalance_mean', 'elapsed_cv_mean', 'Risk scatter by workload');
  renderDistribution(distribution.records || [], metric);
  renderHeatmap(heatmap.records || []);
  renderTable(mergeSummary(summary));
  void loadReport();
  void loadGallery();
}

async function main() {
  await loadMeta();
  await refresh();
  document.getElementById('refreshButton').addEventListener('click', refresh);
  document.getElementById('workloadFilter').addEventListener('change', refresh);
  document.getElementById('batchFilter').addEventListener('change', refresh);
  document.getElementById('heatmapWorkload').addEventListener('change', refresh);
  document.getElementById('heatmapBatch').addEventListener('change', refresh);
  document.getElementById('heatmapMetric').addEventListener('change', refresh);
  document.querySelectorAll('[data-download-target]').forEach(button => {
    button.addEventListener('click', () => {
      triggerSvgDownload(button.dataset.downloadTarget, button.dataset.downloadName || 'chart');
    });
  });
}

main().catch(error => {
  console.error(error);
  document.body.innerHTML = `<pre style="padding:24px;color:#9b1c1c;">${error.message}</pre>`;
});
