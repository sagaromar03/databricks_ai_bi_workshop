// Swap-ready, clickable brand logos. Drop assets/knowit.png / assets/databricks.png (or .svg) to use real logos.
(function(){
  var LINKS = {
    Databricks: 'https://www.databricks.com/',
    Knowit: 'https://www.knowit.eu/'
  };
  function prefix(){ return location.pathname.includes('/pages/') ? '../assets/' : 'assets/'; }
  function dbMark(onDark){
    var txt = onDark ? '#ffffff' : '#1B3139';
    // width trimmed to fit content (glyph + word), no trailing space
    return '<svg class="brand-svg" height="20" width="95" viewBox="0 0 95 20" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Databricks">'
      + '<svg x="0" y="1" width="18" height="18" viewBox="0 0 24 24">'
      + '<path fill="#FF3621" d="M.95 14.184L12 20.403l9.919-5.55v2.21L12 22.662l-10.484-5.96-.565.308v.77L12 24l11.05-6.218v-4.317l-.515-.309L12 19.118l-9.867-5.653v-2.21L12 16.805l11.05-6.218V6.32l-.515-.308L12 11.974 2.647 6.681 12 1.388l7.76 4.368.668-.411v-.566L12 0 .95 6.27v.72L12 13.207l9.919-5.55v2.26L12 15.52 1.516 9.56l-.565.308Z"/></svg>'
      + '<text x="24" y="14.5" font-family="Segoe UI,Helvetica,Arial,sans-serif" font-size="13.5" font-weight="700" fill="'+txt+'">Databricks</text></svg>';
  }
  function knMark(onDark){
    var txt = onDark ? '#ffffff' : '#0a0a0a';
    return '<svg class="brand-svg" height="20" width="52" viewBox="0 0 52 20" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Knowit">'
      + '<text x="0" y="14.5" font-family="Segoe UI,Helvetica,Arial,sans-serif" font-size="13.5" font-weight="700" fill="'+txt+'">Knowit</text></svg>';
  }
  function slot(name, base, onDark){
    var p=prefix(), mark = name==='Databricks'?dbMark(onDark):knMark(onDark);
    return '<a class="brand-slot" href="'+LINKS[name]+'" target="_blank" rel="noopener" aria-label="'+name+'">'
      + '<img class="brand-img" alt="'+name+'" src="'+p+base+'.png" data-step="0" data-base="'+p+base+'" '
      +   'onerror="(function(i){var s=+i.dataset.step;if(s===0){i.dataset.step=1;i.src=i.dataset.base+String.fromCharCode(46)+\'svg\';}else{i.style.display=\'none\';i.nextElementSibling.style.display=\'inline-flex\';}})(this)">'
      + '<span class="brand-fallback" style="display:none">'+mark+'</span></a>';
  }
  function bar(onDark, cls){
    var w=document.createElement('div'); w.className='brandbar '+cls;
    w.innerHTML = slot('Databricks','databricks',onDark)
                + '<span class="brand-x" aria-hidden="true">\u00d7</span>'
                + slot('Knowit','knowit',onDark);
    return w;
  }
  document.addEventListener('DOMContentLoaded', function(){
    var host = document.querySelector('.hero') || document.querySelector('header.top');
    if(host){ host.classList.add('has-brand'); host.appendChild(bar(true,'corner')); }
    var f=document.querySelector('footer');
    if(f){ f.insertBefore(bar(false,'in-footer'), f.firstChild); }
  });
})();
