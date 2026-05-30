# WebShare — robuuste verbindingsopzet

## Het probleem

Mobiele browsers bevriezen JavaScript wanneer het scherm uitgeschakeld wordt. De WebRTC-verbinding valt daarna stuk — niet direct zichtbaar op het bevroren apparaat, maar wel op de andere kant. De bibliotheek detecteert dit via een heartbeat en probeert te herstellen, maar het succes hangt af van welke signaling-backend wordt gebruikt.

Dit document beschrijft de beschikbare opties voor signaling, gerangschikt naar robuustheid en beheerslast.

---

## Vergelijking

| | **PeerJS (cloud)** | **PeerJS (self-hosted)** | **Trystero + Nostr (publiek)** | **Trystero + MQTT (OSB/RabbitMQ)** |
|---|---|---|---|---|
| Herstel na stand-by | Tot 60 s | Configureerbaar | Onbeperkt | Onbeperkt |
| Externe afhankelijkheid | peerjs.com | Geen | Nostr-relays | Geen |
| Eigen infrastructuur nodig | Nee | Ja | Nee | Ja (bestaand) |
| Beheerslast | Geen | Laag | Geen | Nul extra |
| Trystero nodig | Nee | Nee | Ja | Ja |
| Status in demo | Standaard | — | Developer-only | — |

---

## Optie 1 — PeerJS met publieke cloud

**Hoe het werkt:** de ontvanger registreert een peer-ID op peerjs.com. Na verbindingsverlies probeert de bibliotheek dezelfde peer-ID opnieuw te registreren via destroy-en-recreate. peerjs.com houdt het ID beschikbaar voor circa 60 seconden.

**Geschikt voor:** demo's, korte sessies, situaties waarbij stand-by onwaarschijnlijk is.

**Niet geschikt voor:** veldwerk, langere sessies, situaties waarbij telefoons regelmatig op standby gaan.

**Aanbeveling:** gebruik alleen voor demo-doeleinden. Geen productieoptie voor institutioneel gebruik vanwege externe afhankelijkheid en beperkt herstelvenster.

---

## Optie 2 — PeerJS met self-hosted PeerServer

**Hoe het werkt:** zelfde als optie 1, maar de signaling-server draait op eigen infrastructuur. De `alive_timeout`-parameter bepaalt hoe lang een peer-ID na een disconnect gereserveerd blijft — dit kan worden opgehoogd naar minuten in plaats van de ~60 seconden van de publieke cloud.

**Configuratie:**
```js
new PeerJSTransfer({
  peerServer: {
    host: 'webshare-signal.tudelft.nl',
    port: 443,
    path: '/peerjs',
    secure: true
  }
})
```

**Server (Node.js):**
```bash
npx peer --port 9000 --alive_timeout 300000  # 5 minuten
```

**Geschikt voor:** institutioneel gebruik waarbij een langere herstelperiode volstaat. Geen externe afhankelijkheden. Eenvoudige software, lage beheerslast.

**Niet geschikt voor:** situaties waarbij de verbinding na meer dan de geconfigureerde timeout moet kunnen herstellen zonder opnieuw te pairen.

**Infrastructuur:** kleine VM of bestaande Node.js-omgeving. Minimale serverbelasting — PeerServer verwerkt alleen de initiële handshake.

---

## Optie 3 — Trystero met publieke Nostr-relays

**Hoe het werkt:** beide apparaten joinen een gedeelde room op basis van een roomcode. De roomcode verloopt niet. Na een onderbreking — ook na langdurige stand-by — herbinden beide kanten automatisch dezelfde room zodra ze weer online zijn. Geen tijdslimiet.

**Configuratie:** geen — dit is het standaardgedrag van Trystero wanneer geen `relayUrls` is opgegeven.

**Geschikt voor:** situaties waarbij maximale robuustheid bij stand-by vereist is en geen eigen infrastructuur beschikbaar of wenselijk is.

**Aandachtspunt:** afhankelijk van publieke Nostr-relays buiten institutionele controle. Acceptabel voor demo en prototype; minder geschikt voor productie met gevoelige data.

---

## Optie 4 — Trystero met MQTT via OSB of RabbitMQ

**Hoe het werkt:** identiek aan optie 3, maar de signaling verloopt via een eigen MQTT-broker in plaats van publieke Nostr-relays. Trystero's MQTT-backend verwacht een WebSocket-MQTT-endpoint (`wss://`).

**Configuratie:**
```js
import { joinRoom } from 'trystero/mqtt'

const room = joinRoom({
  appId: 'webshare-tudelft',
  relayUrls: ['wss://broker.tudelft.nl/mqtt']
}, sessiecode)
```

**Vereisten OSB:** controleer of de Oracle Service Bus MQTT via WebSocket (`wss://`) ondersteunt in de huidige configuratie. Dit is een vraag voor de middleware-beheerder.

**Vereisten RabbitMQ:** activeer de MQTT-plugin (`rabbitmq-plugins enable rabbitmq_mqtt`) en de Web MQTT-plugin (`rabbitmq_web_mqtt`) voor WebSocket-ondersteuning.

**Geschikt voor:** productie binnen een instelling. Maximale robuustheid, geen externe afhankelijkheden, signaling volledig onder institutionele governance. Bestaande infrastructuur — nul extra beheerslast als MQTT al actief is.

**Dit is de aanbevolen optie voor productiegebruik** als OSB of RabbitMQ beschikbaar zijn met MQTT-ondersteuning.

---

## Aanbeveling

| Situatie | Aanbevolen optie |
|---|---|
| Demo / prototype | Optie 1 of 3 — geen extra setup |
| Productie, OSB of RabbitMQ beschikbaar met MQTT | **Optie 4** — gebruik bestaande infra |
| Productie, alleen PeerServer mogelijk | Optie 2 met hoge `alive_timeout` |

Het stand-by probleem is alleen volledig opgelost in opties 3 en 4 — daar verloopt de reconnect onbeperkt in tijd. Opties 1 en 2 beperken de kwetsbaarheidsduur maar lossen het structureel niet op.

> **Noot over Postgres:** een Postgres-database kan technisch ook als signaling-backend dienen, maar het toevoegen ervan impliceert een server-side applicatie met authenticatie, deployment en beheer. Dat is een architectuurverandering van de huidige offline-first PWA, en een afweging die los staat van het reconnect-probleem.

