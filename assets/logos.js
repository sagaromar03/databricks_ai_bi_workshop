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
    return '<svg class="brand-svg" height="20" width="118" viewBox="0 0 118 20" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Databricks">'
      + '<g fill="none" stroke="#FF3621" stroke-width="1.6" stroke-linejoin="round">'
      + '<path d="M2 6 L9.5 2.1 L17 6 L9.5 9.9 Z"/><path d="M2 10.2 L9.5 14.1 L17 10.2"/><path d="M2 14.4 L9.5 18.3 L17 14.4"/></g>'
      + '<text x="23" y="14.5" font-family="Segoe UI,Helvetica,Arial,sans-serif" font-size="13.5" font-weight="700" fill="'+txt+'">Databricks</text></svg>';
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
