function getVal(id) {
  const elm = document.getElementById(id);
  if (elm == null) {
    console.log("No se ha encontrado #" + id);
    return null;
  }
  if (elm.tagName == "INPUT" && elm.getAttribute("type") == "checkbox") {
    if (elm.checked === false) return false;
    const v = elm.getAttribute("value");
    if (v != null) return v;
    return elm.checked;
  }
  const val = (elm.value ?? "").trim();
  if (val.length == 0) return null;
  const tp = elm.getAttribute("data-type") || elm.getAttribute("type");
  if (tp == "number") {
    const num = Number(val);
    if (isNaN(num)) return null;
    return num;
  }
  return val;
}

function setVal(id, v) {
  const elm = document.getElementById(id);
  if (elm == null) {
    console.log("No se ha encontrado #" + id);
    return null;
  }
  if (elm.tagName == "INPUT" && elm.getAttribute("type") == "checkbox") {
    if (arguments.length == 1) v = elm.defaultChecked;
    elm.checked = v === true;
    return;
  }
  if (arguments.length == 1) {
    v = elm.defaultValue;
  }
  elm.value = v;
}

function getAtt(id, attr) {
  const elm = document.getElementById(id);
  if (elm == null) {
    console.log("No se ha encontrado #" + id);
    return null;
  }
  let v = elm.getAttribute(attr);
  if (v == null) return null;
  v = v.trim();
  if (v.length == 0) return null;
  return v;
}
function setAtt(id, attr, val) {
  const elm = document.getElementById(id);
  if (elm == null) {
    console.log("No se ha encontrado #" + id);
    return null;
  }
  elm.setAttribute(attr, val);
}

function getValDate(id) {
  const n = document.getElementById(id);
  if (n == null) return null;
  if (n.tagName != "INPUT") return null;
  if (n.getAttribute("type") != "date") return null;
  if (n.valueAsDate == null) return n.defaultValue;
  return n.value;
}

function isDate(s) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const dt = new Date(s);
  if (!(dt instanceof Date) || isNaN(dt)) return null;
  const d = dt.getDate();
  const m = dt.getMonth() + 1;
  const y = dt.getFullYear();

  return (
    d === parseInt(s.substring(8, 10), 10) &&
    m === parseInt(s.substring(5, 7), 10) &&
    y === parseInt(s.substring(0, 4), 10)
  );
}

function set_counts(id) {
  const select = document.getElementById(id);
  Array.from(select.options).forEach(o => {
    const txt = (o.getAttribute("data-txt") ?? "Todos");
    const cat = o.value ?? "";
    if (cat.length == 0) o.innerHTML = txt + " (" + document.querySelectorAll("div.evento:not(.hide)").length + ")"
    else o.innerHTML = txt + " (" + document.querySelectorAll("div.evento." + cat + ":not(.hide)").length + ")"
  })
  const op = select.selectedOptions[0];
  const txt = (op.getAttribute("data-txt") ?? "").trim();
  if (txt.length == 0) return null;
  return txt;
}

class FormQuery {
  static clean(obj) {
    if (!INPUT_DATE_SUPPORT) {
      obj.ini = null;
      obj.fin = null;
      return obj;
    }
    if (obj.ini == FormQuery.MIN_DATE && obj.fin == FormQuery.MAX_DATE) {
      obj.ini = null;
      obj.fin = null;
    }
    if (obj.fin == FormQuery.MAX_DATE) obj.fin = null;
    return obj;
  }
  static form() {
    const fch = [getValDate("ini"), getValDate("fin")].sort();
    const d = {
      filtro: getVal("filtro"),
      ini: fch[0],
      fin: fch[1],
    };
    return FormQuery.clean(d);
  }
  static form_to_query() {
    const form = FormQuery.form();
    const qr = [];
    if (form.filtro) qr.push(form.filtro);
    if (form.ini) qr.push(form.ini);
    if (form.fin) qr.push(form.fin);
    let query = qr.length ? "?" + qr.join("&") : "";
    const title = document.querySelector("title");
    const txt = set_counts("filtro");
    if (txt == null) title.textContent = window.__title__;
    else title.textContent = window.__title__ + `: ${txt}`;
    if (document.location.search == query) return;
    const url = document.location.href.replace(/\?.*$/, "");
    history.pushState({}, "", url + query);
  }
  static query_to_form() {
    const query = FormQuery.query();
    setVal("filtro", query.filtro ?? "");
    setVal("ini", query.ini ?? FormQuery.MIN_DATE);
    setVal("fin", query.fin ?? FormQuery.MAX_DATE);
  }
  static query() {
    const search = (() => {
      const q = document.location.search.replace(/^\?/, "");
      if (q.length == 0) return null;
      return q;
    })();
    const d = {
      filtro: null,
      ini: null,
      fin: null,
    };
    if (search == null) return d;
    const dts = [];
    search.split("&").forEach((v) => {
      if (isDate(v)) {
        if (v > FormQuery.MAX_DATE || v < FormQuery.MIN_DATE) return;
        dts.push(v);
        return;
      }
      if (
        document.querySelector('#filtro option[value="' + v + '"]') != null
      )
        d.filtro = v;
    });
    dts.sort();
    if (dts.length > 0) d.ini = dts[0];
    if (dts.length > 1) d.fin = dts[dts.length - 1];
    return FormQuery.clean(d);
  }
}

function getOkSession(d) {
  if (d.ini == null && d.fin == null) return null;
  let ids = new Set(SIN_SESIONES);
  SESIONES.forEach((v, k) => {
    if (d.ini != null && d.ini > k) return;
    if (d.fin != null && d.fin < k) return;
    ids = ids.union(v);
  });
  return ids;
}

function filtrar() {
  const form = FormQuery.form();
  const okSession = getOkSession(form);
  document.querySelectorAll("div.evento").forEach((e) => {
    e.style.display = "";
    if (okSession == null) {
      e.classList.remove("hide");
      return;
    }
    if (okSession.has(e.id)) {
      e.classList.remove("hide");
      return;
    }
    e.classList.add("hide")
  });
  FormQuery.CSS.innerHTML = getCss(form);
  FormQuery.form_to_query();
  return;
}

function getCss(form) {
  if (form.filtro == null) return '';
  return `div.evento:not(.${form.filtro}) {display:none}`;
  const filtro = form.filtro;
  const style = FormQuery.FILTERS.flatMap(arr=>{
    if (!arr.includes(form.filtro)) return [];
    if (arr.length==1) return arr.map(c=> `div.evento:not(.${c})`);
    return arr.filter(c => c != form.filtro).map(c => `div.evento.${c}`)
  })
  return style.join(", ") + " {display:none}";
}

function fixDates() {
  const fin = document.getElementById("fin");
  const i = getValDate("ini");
  const f = getValDate("fin");
  fin.setAttribute("min", i ?? FormQuery.MIN_DATE);
  if (i == null || f == null) return;
  if (i <= f) return;
  fin.value = i;
}

function toLocaleDateString(dt) {
  if (dt == null) dt=new Date()
  const [sdt, shm] = dt.toLocaleDateString(
    'es-ES',
    {year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'}
  ).split(", ");
  const ymd = sdt.split(/\//).reverse().join("-");
  return ymd+' '+shm;
}

function removeOutdated() {
  const now = toLocaleDateString();
  const tdy = now.split(" ")[0];
  const ini = getAtt("ini", "min");
  if (ini > tdy) return;
  setAtt("ini", "min", tdy);
  if (getVal("ini") < tdy) setAtt("ini", "value", tdy);
  let reorder = false;
  const isOK = (e) => {
    if (e.tagName == "DIV" && e.classList.contains("evento")) {
      const lis = e.querySelectorAll("ol.sesiones > li[data-start]");
      if (lis.length == 0) return false;
      const hasOk = Array.from(lis).find(isOK);
      if (hasOk == null) return false;
    }
    const end = e.getAttribute("data-end");
    if (end == null) return true;
    if (end.length==10 && end<tdy) return false;
    if (end.length==16 && end<now) return false;
    return true;
  }
  const rmKO = (e) => {
    if (isOK(e)) return;
    console.log("RM", e);
    e.remove();
    if (e.tagName == "LI") reorder=true;
  }
  document.querySelectorAll("div.evento").forEach(rmKO);
  document.querySelectorAll("li[data-end]").forEach(rmKO);
  document.getElementById("total").textContent = document.querySelectorAll("div.evento").length;
  if (reorder) {
    console.log("Reordenar");
    const new_oreder = Array.from(document.querySelectorAll("div.evento")).map((e, i)=>{
      const li = e.querySelector("li[data-start]");
      const start = li.getAttribute("data-end");
      return [e, start, i];
    }).sort((a, b) => {
      if (a[1] == b[1]) return a[2]-b[2];
      return (a[1] < b[1])?-1:1;
    }).map(x=>x[0]);
    const main = document.querySelector("main");
    new_oreder.forEach(e=>{
      main.appendChild(e);
    })
  }
  const li = document.querySelector("li[data-start]");
  if (li == null) return;
  const dstart = li.getAttribute("data-start");
  console.log("primer evento:", dstart);
  const min = dstart.substring(0, 10);
  setAtt("ini", "min", min);
  if (getVal("ini") < min) setAtt("ini", "value", min);
}

function get_optgroups(id) {
  const optgroups = [];
  const select = document.getElementById(id);
  Array.from(select.getElementsByTagName("optgroup")).forEach(g=>{
    const arr = []
    Array.from(g.getElementsByTagName("option")).forEach(o => {
      const v = o.value.trim();
      if (v.length>0) arr.push(v);
    })
    if (arr.length>0) optgroups.push(arr);
  })
  return optgroups;
}

document.addEventListener("DOMContentLoaded", function () {
  removeOutdated();
  FormQuery.CSS = document.getElementById("jscss");
  FormQuery.MIN_DATE = getAtt("ini", "min");
  FormQuery.MAX_DATE = getAtt("ini", "max");
  FormQuery.FILTERS = get_optgroups("filtro");
  window.__title__ = document.querySelector("title").textContent.trim();
  FormQuery.query_to_form();
  document.getElementById("ini").addEventListener("change", fixDates);
  fixDates();
  document.querySelectorAll("input, select").forEach((i) => {
    i.addEventListener("change", filtrar);
  });
  filtrar();
});
