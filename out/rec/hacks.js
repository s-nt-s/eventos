const INPUT_DATE_SUPPORT=(() => {
    const i = document.createElement("input");
    i.setAttribute("type", "date");
    i.setAttribute("value", "2023-01-01");
    if (i.type !== "date") return false;
    if (i.valueAsDate == null) return false;
    if (!(i.valueAsDate instanceof Date) || isNaN(i.valueAsDate)) return false;
    return true;
})();
if (!Set.prototype.union) {
    Set.prototype.union = function(setB) {
        const unionSet = new Set(this);
        setB.forEach(elem=>unionSet.add(elem));
        return unionSet;
    };
}

document.addEventListener("DOMContentLoaded", function () {
    document.body.classList.add("js");
    if (!INPUT_DATE_SUPPORT) {
        document.body.classList.add("noinputdate");
    }
    if (document.location.protocol != "file:") return;
    Array.from(document.getElementsByTagName("a")).forEach(a => {
        if (a.protocol != "file:") return;
        if (a.pathname.endsWith("/")) {
            a.pathname = a.pathname + "index.html"
            return;
        }
        if (a.pathname.match(/.*\/e\/\d+$/)) {
            a.pathname = a.pathname + ".html"
            return;
        }
    });
});