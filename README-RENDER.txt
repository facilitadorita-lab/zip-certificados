ARQUIVOS PRONTOS PARA O GITHUB E RENDER

DLT
Build Command: npm install
Start Command: node server.js
Health Check Path: /

DLH
Build Command: npm install
Start Command: node server-dlh.js
Health Check Path: /

O arquivo package.json principal atende aos dois servicos.
O dhl-package.json fica apenas como referencia e nao e selecionado automaticamente pelo Render.

IMPORTANTE PARA OS RELATORIOS PDF
- O package.json instala somente o Chromium Headless Shell dentro do proprio pacote.
- O navegador completo nao e necessario para gerar PDF e nao sera baixado.
- Depois de substituir os arquivos, use "Clear build cache & deploy" nos dois servicos.
- Nao altere nem remova o script postinstall.
- PLAYWRIGHT_BROWSERS_PATH nao precisa ser criado no Render; o backend define 0 automaticamente.
- Depois do deploy, confirme no navegador:
  DLT: https://certificados-dlt.onrender.com/status/pdf
  DLH: https://certificados-dlh.onrender.com/dlh/status/pdf
- O resultado correto e: {"ok":true,"servico":"pdf","navegador_instalado":true}

Nunca coloque valores secretos nos arquivos.
Cadastre os valores indicados em .env.example na tela Environment do Render.
