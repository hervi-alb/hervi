# Open Lobby

A public chat room. Visitors pick a username, then everyone online chats in one shared room. Messages show as `username: text`. A search box filters the history. No accounts.

## Run it on your computer

Needs Node 18 or newer.

```
npm install
npm start
```

Open http://localhost:3000 in two browser windows to see them talk to each other.

## Put it online (Render, free tier)

1. Create a free GitHub account if you do not have one, then make a new repository and upload everything in this folder (not `node_modules` or `data`).
2. Create a free account at render.com and choose New, then Web Service, then connect that repository.
3. Render reads `render.yaml`, so the settings fill in on their own. Build command `npm install`, start command `npm start`.
4. When the deploy finishes, Render gives you a public address like `https://open-lobby.onrender.com`. That is your site.

WebSockets work on Render with no extra setup. The page connects to whatever address it is served from.

### Things to know about the free tier

- The service goes to sleep after about 15 minutes with no visitors. The next visitor waits roughly a minute while it wakes up.
- The disk is wiped on every restart or redeploy, so chat history resets. To keep history, use a paid plan with a persistent disk, mount it at `/var/data`, and set the environment variable `DATA_DIR=/var/data`. The `render.yaml` has these lines ready to uncomment.
- Railway and Fly.io work the same way: run `npm start`, expose the port in the `PORT` variable, and attach a volume if you want history to last.

## Your own domain name

Buy a domain from any registrar, then add it in Render under Settings, Custom Domains, and follow the DNS steps it shows.

## Getting found in search

- Submit your site address in Google Search Console and Bing Webmaster Tools so they crawl it.
- The page already has a title, description and a `robots.txt` that allows crawling.
- A chat room has little fixed text, so ranking takes time. Sharing the link in communities helps more than anything in the code.

## Settings (environment variables)

| Variable | Default | What it does |
| --- | --- | --- |
| `PORT` | 3000 | Port the server listens on. Hosts set this for you. |
| `DATA_DIR` | `./data` | Folder where `messages.jsonl` is stored. |

Limits are constants at the top of `server.js`: 20 characters per username, 500 per message, one message per 0.7 seconds per connection, 12 per 10 seconds per IP address, 8 connections per IP.

## Before you open it to strangers

This is a basic, open room. There are no accounts, so usernames are not reserved and anyone can use any name. There is no moderation, no word filter and no way to delete a message except editing `data/messages.jsonl` and restarting. Public chat rooms attract spam and abuse, so plan to watch it, and consider adding a report or mute feature before promoting it widely.
