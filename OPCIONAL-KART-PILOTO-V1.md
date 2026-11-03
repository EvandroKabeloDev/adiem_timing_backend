# Backend MQTT — kart e piloto opcionais (V1)

O backend não exige mais associação em `rt004_session_transponders` para gravar uma volta. Exige apenas:
- payload válido com `transponder_id`;
- transponder cadastrado e ativo;
- sessão ativa correspondente ao dispositivo.

Se houver associação temporal, a volta recebe `session_transponder_id`, `kart_id` e `portal_kart_id` conforme disponíveis. Sem associação, esses campos ficam `NULL`, mas a volta é inserida em `rt004_laps`.

A numeração é sequencial por sessão + transponder, não por associação. Isso evita reiniciar a contagem ao vincular kart/piloto no meio do treino.

Implantar junto com o SQL `optional-transponder-assignment-v1.sql` e com o portal V1 atualizado.
