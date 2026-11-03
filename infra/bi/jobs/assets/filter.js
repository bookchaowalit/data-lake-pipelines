// Progressive enhancement for the companies table: the filter box stays
// hidden without JavaScript, and the full table is always in the HTML.
(function () {
  var form = document.querySelector('[data-filter]');
  if (!form) return;
  var input = form.querySelector('input');
  var rows = Array.prototype.slice.call(document.querySelectorAll('#companies tbody tr'));
  var status = document.getElementById('filter-status');
  var keys = rows.map(function (row) { return row.textContent.toLowerCase(); });
  form.hidden = false;
  form.addEventListener('submit', function (event) { event.preventDefault(); });
  input.addEventListener('input', function () {
    var query = input.value.trim().toLowerCase();
    var shown = 0;
    rows.forEach(function (row, i) {
      var match = !query || keys[i].indexOf(query) !== -1;
      row.hidden = !match;
      if (match) shown += 1;
    });
    status.textContent = shown + ' of ' + rows.length + ' companies';
  });
})();
