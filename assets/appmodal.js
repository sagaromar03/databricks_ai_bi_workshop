/* Shared "Download the Databricks app" QR modal.
   Include on any page: <script src="../assets/appmodal.js?v=1"></script>
   Any link with href ending in #appCard opens the modal in-place — no navigation. */
(function(){
  // Work out where assets/ lives relative to this page, from this script's own URL.
  var me = document.currentScript && document.currentScript.src;
  var base = me ? me.replace(/appmodal\.js.*$/,'') : 'assets/';   // e.g. "../assets/"

  // Styles (scoped to the app modal; matches the homepage modal).
  var css = document.createElement('style');
  css.textContent = [
    '.appmodal{position:fixed;inset:0;z-index:60;display:flex;align-items:center;justify-content:center;padding:20px}',
    '.appmodal[hidden]{display:none}',
    '.appmodal .mb-backdrop{position:absolute;inset:0;background:rgba(13,14,39,.55)}',
    '.appmodal .mb-box{position:relative;background:#fff;border-radius:14px;padding:26px;max-width:540px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,.32)}',
    '.appmodal .mb-box h3{margin:0 0 4px;font-size:19px;color:var(--navy)}',
    '.appmodal .mb-box>p{margin:0 0 18px;font-size:13px;color:var(--grey)}',
    '.appmodal .mb-x{position:absolute;top:10px;right:14px;border:0;background:transparent;font-size:24px;line-height:1;color:var(--grey);cursor:pointer;padding:4px}',
    '.appmodal .mb-x:hover{color:var(--ink)}',
    '.appmodal .qr-row{display:grid;grid-template-columns:repeat(2,1fr);gap:16px}',
    '@media(max-width:520px){.appmodal .qr-row{grid-template-columns:1fr}}',
    '.appmodal .qr-card{background:#fff;border:1px solid var(--border);border-radius:12px;border-top:5px solid var(--navy);padding:18px;text-align:center}',
    '.appmodal .qr-card:nth-child(2){border-top-color:var(--teal)}',
    '.appmodal .qr-card img{width:150px;height:150px;display:block;margin:0 auto 12px;border:1px solid var(--border);border-radius:8px;background:#fff;object-fit:contain}',
    '.appmodal .qr-lab{font-size:14px;font-weight:600;color:var(--navy)}',
    '.appmodal .qr-lab span{display:block;font-weight:400;color:var(--grey);font-size:12px;margin-top:3px}'
  ].join('');
  document.head.appendChild(css);

  // Markup.
  var m = document.createElement('div');
  m.className = 'appmodal'; m.id = 'appModal'; m.hidden = true;
  m.innerHTML =
    '<div class="mb-backdrop" data-close></div>' +
    '<div class="mb-box" role="dialog" aria-modal="true" aria-label="Download the Databricks app">' +
      '<button class="mb-x" data-close aria-label="Close">×</button>' +
      '<h3>Download the Databricks app</h3>' +
      '<p>Scan a code with your phone camera to open the store.</p>' +
      '<div class="qr-row">' +
        '<div class="qr-card"><img src="'+base+'qr_ios.png?v=2" alt="App Store QR code for the Databricks app"><div class="qr-lab">iOS · App Store <span>Point your iPhone camera at the code</span></div></div>' +
        '<div class="qr-card"><img src="'+base+'qr_android.png?v=2" alt="Google Play QR code for the Databricks app"><div class="qr-lab">Android · Google Play <span>Point your Android camera at the code</span></div></div>' +
      '</div>' +
    '</div>';
  document.body.appendChild(m);

  function open(e){ if(e) e.preventDefault(); m.hidden=false; }
  function close(){ m.hidden=true; }
  m.addEventListener('click', function(e){ if(e.target.hasAttribute('data-close')) close(); });
  document.addEventListener('keydown', function(e){ if(e.key==='Escape' && !m.hidden) close(); });

  // Intercept every "Databricks app" link (href ends with #appCard) — open in place.
  document.addEventListener('click', function(e){
    var a = e.target.closest && e.target.closest('a[href$="#appCard"]');
    if(a){ open(e); }
  });
})();
