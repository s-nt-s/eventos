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
      categoria: getVal("categoria"),
      ini: fch[0],
      fin: fch[1],
    };
    return FormQuery.clean(d);
  }
  static form_to_query() {
    const form = FormQuery.form();
    const qr = [];
    if (form.categoria) qr.push(form.categoria);
    if (form.ini) qr.push(form.ini);
    if (form.fin) qr.push(form.fin);
    let query = qr.length ? "?" + qr.join("&") : "";
    const title = document.querySelector("title");
    const select = document.getElementById("categoria");
    Array.from(select.options).forEach(o => {
      const txt = (o.getAttribute("data-txt") ?? "Todos");
      const cat = o.value ?? "";
      if (cat.length == 0) o.innerHTML = txt + " (" + document.querySelectorAll("div.evento:not(.hide)").length + ")"
      else o.innerHTML = txt + " (" + document.querySelectorAll("div.evento." + cat + ":not(.hide)").length + ")"
    })
    const txt = select.selectedOptions[0].getAttribute("data-txt") ?? "".trim();
    if (txt == null || txt.length == 0) title.textContent = window.__title__;
    else title.textContent = window.__title__ + ": " + txt;
    if (document.location.search == query) return;
    const url = document.location.href.replace(/\?.*$/, "");
    history.pushState({}, "", url + query);
  }
  static query_to_form() {
    const query = FormQuery.query();
    setVal("categoria", query.categoria ?? "");
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
      categoria: null,
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
        document.querySelector('#categoria option[value="' + v + '"]') != null
      )
        d.categoria = v;
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
  const categ = form.categoria;
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
  if (categ == null) FormQuery.CSS.innerHTML = "";
  else {
    const style = FormQuery.CATEGORIES.filter(c => c != categ).map(c => "div.evento." + c).join(", ");
    FormQuery.CSS.innerHTML = style + " {display:none}";
  }
  FormQuery.form_to_query();
  return;
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

function removeOutdated() {
  const now = (new Date()).toLocaleDateString(
    'es-ES',
    {year: 'numeric', month: '2-digit', day: '2-digit'}
  ).split(/\//).reverse().join("-");
  const ini = getAtt("ini", "min");
  if (ini >= now) return;
  setAtt("ini", "min", now);
  if (getVal("ini") < now) setAtt("ini", "value", now);
  document.querySelectorAll("div.evento").forEach(e=>{
    const end = e.getAttribute("data-end");
    if (end!=null && end.length==10 && end<now) e.remove();
  })
  document.getElementById("total").textContent = document.querySelectorAll("div.evento").length;
}

document.addEventListener("DOMContentLoaded", function () {
  removeOutdated();
  FormQuery.CSS = document.getElementById("jscss");
  FormQuery.MIN_DATE = getAtt("ini", "min");
  FormQuery.MAX_DATE = getAtt("ini", "max");
  FormQuery.CATEGORIES = Array.from(document.getElementById("categoria").options).flatMap(o => {
    const v = o.value.trim();
    return (v.length == 0) ? [] : v;
  })
  window.__title__ = document.querySelector("title").textContent.trim();
  FormQuery.query_to_form();
  document.getElementById("ini").addEventListener("change", fixDates);
  fixDates();
  document.querySelectorAll("input, select").forEach((i) => {
    i.addEventListener("change", filtrar);
  });
  filtrar();
});
