function add_class_to_img(i) {
    if (!i.complete || i.naturalWidth == 0) return;
    i.classList.add("loaded");
}

document.addEventListener("DOMContentLoaded", function () {
  document.querySelectorAll("img").forEach((i) => {
    i.addEventListener("error", (e) => e.target.classList.add("loaderror"));
    if (i.complete && i.naturalWidth !== 0) add_class_to_img(i);
    else i.addEventListener("load", (e) => add_class_to_img(e.target));
  });
});
