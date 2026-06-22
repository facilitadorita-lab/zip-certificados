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

Nunca coloque valores secretos nos arquivos.
Cadastre os valores indicados em .env.example na tela Environment do Render.
