// Robust notebook download.
// On http/https (e.g. GitHub Pages) this fetches the file and saves it with a proper filename.
// On file:// browsers block both fetch and the download attribute, so we open it in a new tab
// and hint the user to use "Save As" (nothing else is technically possible locally).
(function(){
  var isFile = location.protocol === 'file:';
  function saveBlob(blob, filename){
    var url = URL.createObjectURL(blob), a = document.createElement('a');
    a.href = url; a.download = filename; document.body.appendChild(a); a.click();
    setTimeout(function(){ URL.revokeObjectURL(url); a.remove(); }, 1500);
  }
  document.addEventListener('click', function(e){
    var a = e.target.closest('a[href$=".ipynb"]');
    if(!a) return;
    var href = a.getAttribute('href');
    var name = href.split('/').pop() || 'notebook.ipynb';
    if(isFile){
      // Can't force a download from the local filesystem; open in a new tab for Save As.
      // (This limitation disappears once the site is hosted over http/https.)
      return; // let the browser follow the link normally in a new tab via target
    }
    e.preventDefault();
    fetch(href).then(function(r){ if(!r.ok) throw 0; return r.blob(); })
      .then(function(b){ saveBlob(b, name); })
      .catch(function(){ window.open(href, '_blank'); });
  });
  // On file://, make notebook links open in a new tab so the user isn't stuck on a JSON screen
  if(isFile){
    document.addEventListener('DOMContentLoaded', function(){
      document.querySelectorAll('a[href$=".ipynb"]').forEach(function(a){ a.target='_blank'; });
    });
  }
})();
