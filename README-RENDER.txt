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
- Os PDFs sao gerados diretamente pelo PDFKit, sem navegador.
- Nao existe download de navegador durante o build.
- Depois de substituir os arquivos, use "Clear build cache & deploy" nos dois servicos.
- Depois do deploy, confirme no navegador:
  DLT: https://certificados-dlt.onrender.com/status/pdf
  DLH: https://certificados-dlh.onrender.com/dlh/status/pdf
- O resultado correto e: {"ok":true,"servico":"pdf","motor":"pdfkit"}

Nunca coloque valores secretos nos arquivos.
Cadastre os valores indicados em .env.example na tela Environment do Render.

FALLBACK DE LEITURA DLH COM IA
- O parser original continua sendo sempre a primeira tentativa.
- O leitor de layout alternativo e tentado somente quando o original nao retorna 3 pontos de umidade e 4 de temperatura.
- A IA so e chamada quando OPENAI_API_KEY estiver configurada; sem essa chave o backend segue funcionando apenas com os parsers locais.
- OPENAI_DLH_MODEL: modelo usado no fallback, padrao gpt-4.1-mini.
- DLH_AI_FALLBACK_ENABLED: true ou false, padrao true.
- DLH_AI_TIMEOUT_MS: tempo maximo da leitura da IA em milissegundos, padrao 60000.
- DRIVE_REQUEST_TIMEOUT_MS: limite para listar ou baixar um PDF do Drive, padrao 45000.
- PROCESSAMENTO_ARQUIVO_TIMEOUT_MS: limite total para processar um PDF, padrao 150000.
- EXTERNAL_REQUEST_TIMEOUT_MS: limite geral para chamadas externas, padrao 60000.
- A IA precisa retornar o conjunto completo; pontos parciais sao descartados e nao alteram o resultado do parser original.
- Falhas de download, leitura ou gravacao ficam registradas como ERRO para o lote finalizar e poder ser reprocessado depois.

SUPORTE POR E-MAIL E TELEGRAM
- SUPPORT_TO_EMAIL: e-mail que recebe os novos tickets.
- SUPPORT_FROM_EMAIL: remetente verificado no Resend.
- SUPPORT_RESEND_API_KEY: chave de envio do Resend.
- TELEGRAM_BOT_TOKEN: token do bot criado no BotFather.
- TELEGRAM_CHAT_ID: chat autorizado a receber e responder tickets.
- TELEGRAM_WEBHOOK_SECRET: segredo longo usado pelo Telegram no webhook.
- O webhook de resposta fica somente no DLT:
  https://zip-certificados.onrender.com/suporte/telegram/webhook
- Para responder ao usuario, use "Responder" na mensagem original do ticket no Telegram.
