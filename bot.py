import os
import re
import json
import asyncio
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord import app_commands

import asyncpg
import redis.asyncio as aioredis
from mcrcon import MCRcon
from dotenv import load_dotenv

# Optional: one-time migration deps
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ── Load environment ─────────────────────────────────────────────────────────
load_dotenv()
TOKEN                   = os.getenv("DISCORD_TOKEN")
GUILD_ID                = int(os.getenv("GUILD_ID"))
ALLOWED_ROLE_ID         = int(os.getenv("ALLOWED_ROLE_ID"))

ANTHRATA_PG_CONN        = os.getenv("ANTHRATA_PG_CONN")

REDIS_URL               = os.getenv("REDIS_URL")
RCON_HOST               = os.getenv("RCON_HOST")
RCON_PORT               = int(os.getenv("RCON_PORT"))
RCON_PASS               = os.getenv("RCON_PASS")

KEYSTONE_POST_CHANNEL_ID = int(os.getenv("KEYSTONE_POST_CHANNEL_ID", "0"))
KEYSTONE_EMOJI_ID        = int(os.getenv("KEYSTONE_EMOJI_ID", "0"))
STAFF_ROLE_IDS = {
    int(os.getenv("STAFF_ROLE_ID_1", "0")),
    int(os.getenv("STAFF_ROLE_ID_2", "0")),
}

RUN_SHEETS_MIGRATION    = os.getenv("RUN_SHEETS_MIGRATION", "false").lower() == "true"
SHEET_NAME              = os.getenv("SHEET_NAME")
GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

# ── Regex ────────────────────────────────────────────────────────────────────
AID_REGEX = re.compile(r"^\d{3}-\d{3}-\d{3}$", re.ASCII)

# ── Bot & clients ────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

redis_client: aioredis.Redis | None = None
db_pool: asyncpg.Pool | None = None

LOCK_WHISPER = "You have not unlocked this keystone species yet."
keystone_species_cache: list[str] = []  # raw names

# ── Bootstrap DDL (create tables if missing) ─────────────────────────────────
DDL_STATEMENTS = [
    """
    create table if not exists public.players (
      player_id     bigserial primary key,
      discord_id    text unique not null,
      discord_tag   text,
      alderon_id    text unique,
      created_at    timestamptz not null default now()
    );
    """,
    """
    create table if not exists public.keystone_species (
      species       text primary key,
      enabled       boolean not null default true
    );
    """,
    """
    create table if not exists public.player_keystone_status (
      player_id     bigint not null references public.players(player_id) on delete cascade,
      species       text not null references public.keystone_species(species) on delete cascade,
      is_unlocked   boolean not null default false,
      unlocked_at   timestamptz,
      constraint player_keystone_status_pk primary key (player_id, species)
    );
    """,
    """
    create table if not exists public.player_items (
      player_id     bigint not null references public.players(player_id) on delete cascade,
      item_code     text not null,
      quantity      integer not null check (quantity >= 0),
      created_at    timestamptz not null default now(),
      constraint player_items_pk primary key (player_id, item_code)
    );
    """,
    """
    create table if not exists public.keystone_redeem_txn (
      txn_id        bigserial primary key,
      player_id     bigint not null references public.players(player_id) on delete cascade,
      species       text not null references public.keystone_species(species),
      spent_item_code text not null,
      spent_qty     integer not null check (spent_qty = 1),
      created_at    timestamptz not null default now(),
      constraint keystone_redeem_txn_unique unique (player_id, species)
    );
    """,
    """
    create table if not exists public.keystone_grant_audit (
      audit_id      bigserial primary key,
      grant_ts      timestamptz not null default now(),
      granter_discord_id text not null,
      granter_discord_tag text,
      recipient_discord_id text not null,
      recipient_discord_tag text,
      channel_id    text not null,
      message_id    text not null,
      emoji_id      text not null,
      item_code     text not null,
      quantity      integer not null check (quantity = 1)
    );
    """,
    """
    create table if not exists public.sheets_archive (
      archive_id    bigserial primary key,
      discord_id    text,
      discord_tag   text,
      alderon_id    text,
      joined_at     date,
      is_14_days    boolean,
      archived_at   timestamptz not null default now()
    );
    """,
    # Seed species; exact raw names must match in-game labels
    """
    insert into public.keystone_species (species) values
      ('Giant Salamander'),
      ('Tyrannosaurus (PT)'),
      ('Spinosaurus'),
      ('Rhamphorynchus'),
      ('Compsognathus'),
      ('Giganotosaurus'),
      ('Tyrannotitan'),
      ('Torvosaurus'),
      ('Iguanodon')
    on conflict (species) do nothing;
    """
]

async def bootstrap_schema():
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for ddl in DDL_STATEMENTS:
                await conn.execute(ddl)
    print("[DB] Schema bootstrap complete.")

# ── Cache loader ─────────────────────────────────────────────────────────────
async def load_keystone_species_cache():
    global keystone_species_cache
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            select species
            from public.keystone_species
            where enabled = true
            order by species
        """)
        keystone_species_cache = [r["species"] for r in rows]
        print(f"[CACHE] Keystone species loaded: {len(keystone_species_cache)}")

def has_staff_role(member: discord.Member) -> bool:
    return any(r.id in STAFF_ROLE_IDS for r in member.roles)

# ── One-time Sheets migration (embedded) ─────────────────────────────────────
async def run_one_time_migration():
    if not RUN_SHEETS_MIGRATION:
        return

    print("[MIGRATION] Starting one-time migration from Google Sheets...")
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds_info = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
        gc = gspread.authorize(creds)
        sheet = gc.open(SHEET_NAME).sheet1

        col_a = sheet.col_values(1)  # discord_id
        col_b = sheet.col_values(2)  # discord_tag
        col_c = sheet.col_values(3)  # alderon_id
        col_d = sheet.col_values(4)  # joined_at (YYYY-MM-DD)
        col_e = sheet.col_values(5)  # is_14 flag

        start_idx = 1
        if col_a and not col_a[0].strip().isdigit():
            start_idx = 2

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Ensure species cache is loaded
                await load_keystone_species_cache()

                for idx in range(start_idx-1, max(map(len, [col_a, col_b, col_c, col_d, col_e]))):
                    discord_id  = (col_a[idx].strip() if idx < len(col_a) else "")
                    discord_tag = (col_b[idx].strip() if idx < len(col_b) else "")
                    alderon_id  = (col_c[idx].strip() if idx < len(col_c) else "")
                    joined_str  = (col_d[idx].strip() if idx < len(col_d) else "")
                    is_14_raw   = (col_e[idx].strip().upper() if idx < len(col_e) else "")

                    if not discord_id:
                        continue

                    # Upsert player
                    player = await conn.fetchrow("""
                        insert into public.players (discord_id, discord_tag, alderon_id, created_at)
                        values ($1, $2, nullif($3, ''), now())
                        on conflict (discord_id) do update set
                          discord_tag = excluded.discord_tag,
                          alderon_id  = excluded.alderon_id
                        returning player_id
                    """, discord_id, discord_tag, alderon_id)
                    player_id = player["player_id"]

                    # Initialize keystone status (FALSE for all enabled species)
                    for sname in keystone_species_cache:
                        await conn.execute("""
                            insert into public.player_keystone_status (player_id, species, is_unlocked)
                            values ($1, $2, false)
                            on conflict (player_id, species) do nothing
                        """, player_id, sname)

                    # Archive sheet row
                    joined_at = None
                    if joined_str:
                        try:
                            joined_at = datetime.strptime(joined_str, "%Y-%m-%d").date()
                        except:
                            joined_at = None
                    is_14_days = True if is_14_raw == "TRUE" else False if is_14_raw == "FALSE" else None

                    await conn.execute("""
                        insert into public.sheets_archive (discord_id, discord_tag, alderon_id, joined_at, is_14_days)
                        values ($1, $2, nullif($3, ''), $4, $5)
                    """, discord_id, discord_tag, alderon_id or None, joined_at, is_14_days)

        print("[MIGRATION] Completed. Remove RUN_SHEETS_MIGRATION and migration code after verification.")
    except Exception as e:
        print(f"[MIGRATION] Failed: {e}")

# ── Ready/setup ──────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    global redis_client, db_pool

    guild = discord.Object(id=GUILD_ID)
    bot.tree.copy_global_to(guild=guild)
    await bot.tree.sync(guild=guild)
    print(f"[READY] Logged in as {bot.user}")

    if db_pool is None:
        db_pool = await asyncpg.create_pool(dsn=ANTHRATA_PG_CONN, min_size=1, max_size=5, timeout=10)
        print("[DB] Connected pool")

    await bootstrap_schema()
    await load_keystone_species_cache()

    if RUN_SHEETS_MIGRATION:
        await run_one_time_migration()
        await load_keystone_species_cache()  # reload in case species added during migration

    if redis_client is None:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        print("[REDIS] Connected")

    if not hasattr(bot, "redis_heartbeat"):
        bot.redis_heartbeat = bot.loop.create_task(redis_heartbeat())

    if not hasattr(bot, "respawn_task"):
        print("[READY] Starting Redis respawn listener…")
        bot.respawn_task = bot.loop.create_task(handle_respawn_events())

async def redis_heartbeat():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            if redis_client:
                pong = await redis_client.ping()
                ts = datetime.now(timezone.utc).isoformat()
                print(f"[REDIS] Heartbeat OK at {ts}" if pong else f"[REDIS] Heartbeat unexpected at {ts}")
        except Exception as e:
            print(f"[REDIS] Heartbeat failed: {e}")
            try:
                redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
                print("[REDIS] Reconnected")
            except Exception as re_e:
                print(f"[REDIS] Reconnect failed: {re_e}")
        await asyncio.sleep(300)

# ── /anthraxlink preserved: SQL + whitelist ──────────────────────────────────
@app_commands.command(name="anthraxlink", description="Register or update your Alderon ID (format 000-000-000)")
@app_commands.describe(aid="Your Alderon ID in format 000-000-000")
async def anthraxlink(interaction: discord.Interaction, aid: str):
    member = interaction.user

    if ALLOWED_ROLE_ID not in [r.id for r in member.roles]:
        return await interaction.response.send_message(
            "You cannot link your account until your application has been accepted.",
            ephemeral=True
        )

    if not AID_REGEX.match(aid):
        return await interaction.response.send_message(
            "Invalid AID format. Use `000-000-000`.",
            ephemeral=True
        )

    discord_id = str(member.id)
    discord_tag = str(member)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                insert into public.players (discord_id, discord_tag, alderon_id)
                values ($1, $2, $3)
                on conflict (discord_id)
                do update set discord_tag = excluded.discord_tag, alderon_id = excluded.alderon_id
            """, discord_id, discord_tag, aid)

    try:
        with MCRcon(RCON_HOST, RCON_PASS, port=RCON_PORT) as rcon:
            cmd = f"/whitelist {aid}"
            print(f"[RCON] → {cmd!r}")
            resp = rcon.command(cmd)
            print(f"[RCON] ← {resp!r}")
    except Exception as e:
        print(f"[ERROR] RCON whitelist failed for {aid}: {e}")

    return await interaction.response.send_message(f"✅ Linked AID `{aid}`", ephemeral=True)

bot.tree.add_command(anthraxlink)

# ── Staff reaction → grant item + audit ──────────────────────────────────────
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.channel_id != KEYSTONE_POST_CHANNEL_ID:
        return
    if payload.emoji.id != KEYSTONE_EMOJI_ID:
        return

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    reactor = guild.get_member(payload.user_id)
    if not reactor or not has_staff_role(reactor):
        return

    channel = guild.get_channel(KEYSTONE_POST_CHANNEL_ID)
    try:
        message = await channel.fetch_message(payload.message_id)
    except Exception as e:
        print(f"[REDEEM] Failed to fetch message: {e}")
        return

    author = message.author
    granter_id = str(reactor.id)
    granter_tag = str(reactor)
    recipient_id = str(author.id)
    recipient_tag = str(author)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Ensure recipient exists
            player = await conn.fetchrow("""
                insert into public.players (discord_id, discord_tag)
                values ($1, $2)
                on conflict (discord_id)
                do update set discord_tag = excluded.discord_tag
                returning player_id
            """, recipient_id, recipient_tag)
            player_id = player["player_id"]

            # Grant item
            await conn.execute("""
                insert into public.player_items (player_id, item_code, quantity)
                values ($1, 'keystone_redeem', 1)
                on conflict (player_id, item_code)
                do update set quantity = public.player_items.quantity + 1
            """, player_id)

            # Audit
            await conn.execute("""
                insert into public.keystone_grant_audit (
                    granter_discord_id, granter_discord_tag,
                    recipient_discord_id, recipient_discord_tag,
                    channel_id, message_id, emoji_id, item_code, quantity
                )
                values ($1, $2, $3, $4, $5, $6, $7, 'keystone_redeem', 1)
            """, granter_id, granter_tag, recipient_id, recipient_tag,
                 str(payload.channel_id), str(payload.message_id), str(payload.emoji.id))

    try:
        await message.reply(f"{author.mention} has been granted 1 keystone_redeem by {reactor.mention}.")
    except Exception as e:
        print(f"[REDEEM] Failed to reply: {e}")

# ── /redeem_keystone: dropdown + atomic unlock ───────────────────────────────
class RedeemKeystone(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="redeem_keystone", description="Redeem 1 keystone item to unlock a species")
    @app_commands.describe(species_name="Choose a keystone species to unlock")
    async def redeem_keystone(self, interaction: discord.Interaction, species_name: str):
        await interaction.response.defer(ephemeral=True)

        valid_names = set(keystone_species_cache)
        if species_name not in valid_names:
            await interaction.followup.send("Invalid species name. Use the provided list.", ephemeral=True)
            return

        player_discord_id = str(interaction.user.id)

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Ensure player exists
                player = await conn.fetchrow("""
                    select player_id from public.players where discord_id = $1
                """, player_discord_id)
                if not player:
                    player = await conn.fetchrow("""
                        insert into public.players (discord_id, discord_tag)
                        values ($1, $2)
                        returning player_id
                    """, player_discord_id, str(interaction.user))
                player_id = player["player_id"]

                # Check current status
                status = await conn.fetchrow("""
                    select is_unlocked from public.player_keystone_status
                    where player_id = $1 and species = $2
                """, player_id, species_name)
                if status and status["is_unlocked"]:
                    await interaction.followup.send("You have already unlocked this species. Pick another.", ephemeral=True)
                    return
                if not status:
                    await conn.execute("""
                        insert into public.player_keystone_status (player_id, species, is_unlocked)
                        values ($1, $2, false)
                        on conflict (player_id, species) do nothing
                    """, player_id, species_name)

                # Lock item row and check balance
                item = await conn.fetchrow("""
                    select quantity from public.player_items
                    where player_id = $1 and item_code = 'keystone_redeem'
                    for update
                """, player_id)
                if not item or item["quantity"] < 1:
                    await interaction.followup.send("You don't have a keystone_redeem item to spend.", ephemeral=True)
                    return

                # Spend and unlock
                await conn.execute("""
                    update public.player_items
                    set quantity = quantity - 1
                    where player_id = $1 and item_code = 'keystone_redeem' and quantity >= 1
                """, player_id)

                await conn.execute("""
                    update public.player_keystone_status
                    set is_unlocked = true, unlocked_at = now()
                    where player_id = $1 and species = $2
                """, player_id, species_name)

                await conn.execute("""
                    insert into public.keystone_redeem_txn (player_id, species, spent_item_code, spent_qty)
                    values ($1, $2, 'keystone_redeem', 1)
                    on conflict (player_id, species) do nothing
                """, player_id, species_name)

        await interaction.followup.send(f"Keystone unlocked: {species_name}. You may now play this species.", ephemeral=True)

async def setup_redeem_cog():
    cog = RedeemKeystone(bot)
    bot.tree.add_command(cog.redeem_keystone)

@bot.event
async def setup_hook():
    await setup_redeem_cog()

# ── Autocomplete for species dropdown ────────────────────────────────────────
@anthraxlink.autocomplete("aid")
async def aid_autocomplete(interaction: discord.Interaction, current: str):
    # Not used; placeholder to avoid Discord warnings
    return []

@RedeemKeystone.redeem_keystone.autocomplete("species_name")
async def species_autocomplete(interaction: discord.Interaction, current: str):
    current_lower = current.lower()
    choices = [name for name in keystone_species_cache if current_lower in name.lower()]
    return [app_commands.Choice(name=name, value=name) for name in choices[:25]]

# ── Respawn enforcement (Redis → RCON) ───────────────────────────────────────
async def get_player_id_by_aid(conn: asyncpg.Connection, alderon_id: str) -> int | None:
    if not alderon_id:
        return None
    row = await conn.fetchrow("select player_id from public.players where alderon_id = $1", alderon_id)
    return row["player_id"] if row else None

async def is_unlocked(conn: asyncpg.Connection, player_id: int, species_name: str) -> bool:
    row = await conn.fetchrow("""
        select pks.is_unlocked
        from public.player_keystone_status pks
        where pks.player_id = $1 and pks.species = $2
    """, player_id, species_name)
    return bool(row and row["is_unlocked"])

async def handle_respawn_events():
    global redis_client, db_pool
    if redis_client is None:
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

    pubsub = redis_client.pubsub()
    await pubsub.subscribe("pot_events")
    print("[REDIS] Subscribed to pot_events channel")

    async for msg in pubsub.listen():
        if msg.get("type") != "message":
            continue

        try:
            payload = json.loads(msg["data"])
        except Exception as e:
            print(f"[ERROR] JSON parse failed: {e}")
            continue

        if payload.get("event") != "PlayerRespawn":
            continue

        try:
            desc = payload["data"]["embeds"][0]["description"]
        except Exception:
            print("[PARSER] Missing embed description; skipping.")
            continue

        details = {}
        for line in desc.splitlines():
            clean = line.strip().replace("**", "")
            if ": " in clean:
                k, v = clean.split(": ", 1)
                details[k] = v

        species_name = details.get("DinosaurType")
        player_aid   = details.get("PlayerAlderonId")

        if not species_name or not player_aid:
            continue

        # Check if species is keystone and enabled
        is_keystone = False
        unlocked = False
        async with db_pool.acquire() as conn:
            ks = await conn.fetchrow("""
                select 1 from public.keystone_species where species = $1 and enabled = true
            """, species_name)
            if ks:
                is_keystone = True
                player_id = await get_player_id_by_aid(conn, player_aid)
                if player_id is not None:
                    unlocked = await is_unlocked(conn, player_id, species_name)

        if not is_keystone:
            continue
        if unlocked:
            continue

        try:
            with MCRcon(RCON_HOST, RCON_PASS, port=RCON_PORT) as rcon:
                whisper_cmd = f"/whisper {player_aid} {LOCK_WHISPER}"
                growth_cmd  = f"/setattr {player_aid} GrowthPerSecond 0"
                speed_cmd   = f"/setattr {player_aid} MovementSpeedMultiplier 0"

                print(f"[RCON] → {whisper_cmd!r}")
                rcon.command(whisper_cmd)

                print(f"[RCON] → {growth_cmd!r}")
                rcon.command(growth_cmd)

                print(f"[RCON] → {speed_cmd!r}")
                rcon.command(speed_cmd)
        except Exception as e:
            print(f"[ERROR] RCON failure for AID {player_aid}: {e}")

# ── Run bot ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot.run(TOKEN)