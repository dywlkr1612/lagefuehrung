# Lagefuehrung – Cloud (Cloudflare Worker)

Einsatzfuehrungs- und Lagedarstellungstool. Diese Cloud-Variante laeuft als
Cloudflare Worker mit einem Durable Object ("die Lage") und synchronisiert alle
Arbeitsplaetze live ueber das Internet.

## In wenigen Sekunden deployen

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/DEIN-BENUTZERNAME/DEIN-REPO)

> Ersetze im obigen Link `DEIN-BENUTZERNAME/DEIN-REPO` durch dein eigenes
> GitHub-Repository. Alternativ funktioniert der Button gar nicht zwingend:
> Du kannst auch einfach die **URL dieses Repositorys** im Cloudflare-Dashboard
> unter "Workers & Pages" -> "Create" einfuegen.

Cloudflare liest die Datei `wrangler.toml`, legt das Durable Object `LageRoom`
inklusive SQLite-Speicher **automatisch** an, richtet das Binding `LAGE` ein und
deployt den Worker. Es ist **kein** lokales Node.js und **kein** lokales Wrangler
noetig – alles laeuft auf Cloudflares Servern.

## Enthaltene Dateien

- `worker.js` – der Worker inkl. Durable Object und eingebetteter Oberflaeche
- `wrangler.toml` – Konfiguration (Binding `LAGE` -> Klasse `LageRoom`,
  SQLite-Migration `v1`)
- `package.json` – Projektkennung (Deploy-Skript)

## Nach dem Deploy

- Die angezeigte Adresse (`https://lagefuehrung.<name>.workers.dev`) an alle
  Arbeitsplaetze verteilen.
- Melde-Formular fuers Handy: dieselbe Adresse mit `/melden` am Ende.

Ausfuehrliche Bedien- und Einrichtungshinweise stehen in `README_cloud.md`
bzw. der iPhone-Anleitung `ANLEITUNG_iPhone_GitHub.md`.
