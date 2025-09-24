const params = new URLSearchParams(location.hash.slice(1));
const token = params.get("access_token");
console.info("token:", token);
const error = params.get("error");
if (token) {
  const input = document.querySelector('input#token');
  input.value = token;
  input.select();
  document.title = "Authorization successful — Throttled Backup";
  document.querySelector('#no_auth').style.display = 'none';
  document.querySelector('#access_denied').style.display = 'none';
} else if (error) {
  const errorElmnt = document.querySelector('p#error');
  errorElmnt.innerText = "Details: " + error;
  document.title = "Authorization denied — Throttled Backup";
  document.querySelector('#no_auth').style.display = 'none';
  document.querySelector('#authorized').style.display = 'none';
} else {
  document.querySelector('#authorized').style.display = 'none';
  document.querySelector('#access_denied').style.display = 'none';
}
