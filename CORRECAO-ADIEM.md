# Correção ADIEM Timing Backend

## O que foi corrigido
- Removida a segunda declaração duplicada de `processLap()` e `getNextLapNumber()`.
- O fluxo MQTT usa o horário `passageAt` capturado no recebimento.
- A associação é resolvida em `rt004_session_transponders` pela sessão, transponder e intervalo temporal.
- A gravação usa `session_transponder_id`, `portal_kart_id` e, quando disponível, `rt004_kart_id` em `kart_id`.
- A validação de transponder ativo continua sendo aplicada.

## Publicação
Substitua o `server.js` atual pelo arquivo deste pacote e mantenha o `package.json` existente.
Não altere as variáveis de ambiente já configuradas no EasyPanel.
Faça commit e aguarde o deploy.

## Teste
1. Inicie uma sessão pelo Live Timing.
2. Confirme que existe uma linha vigente em `rt004_session_transponders` para a sessão e transponder.
3. Passe o transponder na linha.
4. Confira os logs `[LAP] VOLTA RECEBIDA E GRAVADA`.
5. Confira `rt004_laps`, incluindo `session_transponder_id`, `portal_kart_id` e `kart_id`.

A associação temporal precisa existir antes da passagem. Este pacote não cria associações automaticamente.
