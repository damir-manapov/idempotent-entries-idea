import {Random} from 'reliable-random';

/* 
  Idempotent, O(1) test data generator for profile-based raw records
  ------------------------------------------------------------------
  ✓ Deterministic per profileId / recordId using 64-bit hashing and PRNG
  ✓ No pre-generation of profiles; everything is computed on the fly
  ✓ Configurable frequency buckets: control expected repeats per profile
  ✓ Controlled distortions (swap names, typos, transliteration, etc.)
  ✓ Time slicing: generate across a date range with deterministic spread
*/

// Add BigInt JSON serialization support
declare global {
  interface BigInt {
    toJSON(): string;
  }
}

if (!BigInt.prototype.toJSON) {
  BigInt.prototype.toJSON = function() {
    return this.toString();
  };
}

// ---------- Types ----------
export interface FrequencyBucket {
  weight: number;
  repeatMultiplier: number;
}

export interface DistortionRates {
  swapFirstLast: number;
  transliterate: number;
  typo: number;
}

export interface DateSpreadConfig {
  start: Date;
  end: Date;
}

export interface GeneratorConfig {
  profileSpaceSize: bigint;
  buckets: FrequencyBucket[];
  distortions: DistortionRates;
  dateSpread: DateSpreadConfig;
  pools: Pools;
}

export interface Profile {
  profileId: bigint;
  firstName: string;
  lastName: string;
  phones: string[];
  emails: string[];
  logins: string[];
  locale: string;
}

export interface RawRecord {
  recordIndex: bigint;
  profileId: bigint;
  variantIndex: number;
  firstName: string;
  lastName: string;
  email: string;
  phone: string;
  login: string;
  pointOfSale: string;
  city: string;
  channel: string;
  amount: number;
  timestamp: string;
}

export interface Pools {
  firstNames: WeightedPool<string>;
  lastNames: WeightedPool<string>;
  cities: WeightedPool<string>;
  channels: WeightedPool<string>;
  pos: WeightedPool<string>;
}

export interface WeightedPool<T> {
  values: T[];
  weights?: number[];
}

// ---------- Utilities: 64-bit hashing & PRNG ----------
export function fnv1a64(input: string | bigint): bigint {
  let data: Uint8Array;
  if (typeof input === 'bigint') {
    const buf = new ArrayBuffer(8);
    const view = new DataView(buf);
    view.setBigUint64(0, input);
    data = new Uint8Array(buf);
  } else {
    data = new TextEncoder().encode(input);
  }
  let hash = 0xcbf29ce484222325n; // offset basis
  const prime = 0x100000001b3n; // FNV prime
  for (let i = 0; i < data.length; i++) {
    hash ^= BigInt(data[i]);
    hash = (hash * prime) & 0xFFFFFFFFFFFFFFFFn;
  }
  return hash;
}

interface ISplitMix64 {
  nextUint64(): bigint;
  nextFloat(): number;
  nextIntExclusive(maxExclusive: number): number;
}

export class SplitMix64 implements ISplitMix64 {
  private rand: Random;
  constructor(seed: bigint) {
    this.rand = new Random(seed, seed);
  }
  nextUint64(): bigint {
    return this.rand._random_b();
  }
  nextFloat(): number {
    return this.rand.random();
  }
  nextIntExclusive(maxExclusive: number): number {
    return this.rand.randint(maxExclusive);
  }
  nextIntInclusive(maxInclusive: number): number {
    return this.nextIntExclusive(maxInclusive + 1);
  }
}

export function flatPick<T>(rng: SplitMix64, arr: T[]): T | undefined {
  if (!arr.length) {
    return undefined;
  }

  return arr[rng.nextIntExclusive(arr.length)];
}

export function weightedPick<T>(rng: SplitMix64, pool: WeightedPool<T>): T | undefined {
  const { values, weights } = pool;
  if (!weights || weights.length === 0) {
    return flatPick(rng, values);
  }
  let total = 0;
  for (const w of weights) total += w;
  let r = rng.nextFloat() * total;
  for (let i = 0; i < values.length; i++) {
    r -= weights[i];
    if (r <= 0) return values[i];
  }
  return values[values.length - 1];
}

export function classifyBucket(profileId: bigint, buckets: FrequencyBucket[]): FrequencyBucket {
  const seed = fnv1a64(profileId);
  const rng = new SplitMix64(seed);
  const total = buckets.reduce((s, b) => s + b.weight, 0);
  let r = rng.nextFloat() * total;
  for (const b of buckets) {
    r -= b.weight;
    if (r <= 0) return b;
  }
  return buckets[buckets.length - 1];
}

// ---------- Distortions ----------
const cyrToLatMap: Record<string, string> = {
  'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Zh','З':'Z','И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R','С':'S','Т':'T','У':'U','Ф':'F','Х':'Kh','Ц':'Ts','Ч':'Ch','Ш':'Sh','Щ':'Sch','Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'Yu','Я':'Ya',
  'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'
};

function transliterateCyrillicToLatin(s: string): string {
  return s.split('').map(ch => cyrToLatMap[ch] ?? ch).join('');
}

function randomTypo(rng: SplitMix64, s: string): string {
  if (s.length === 0) return s;
  const ops = ['swap', 'delete', 'insert', 'replace'] as const;
  const op = ops[rng.nextIntExclusive(ops.length)];
  const alphabet = 'abcdefghijklmnopqrstuvwxyz';
  switch (op) {
    case 'swap': {
      if (s.length < 2) return s; 
      const i = rng.nextIntExclusive(s.length - 1);
      const arr = s.split('');
      [arr[i], arr[i+1]] = [arr[i+1], arr[i]];
      return arr.join('');
    }
    case 'delete': {
      const i = rng.nextIntExclusive(s.length);
      return s.slice(0, i) + s.slice(i+1);
    }
    case 'insert': {
      const i = rng.nextIntExclusive(s.length + 1);
      const ch = alphabet[rng.nextIntExclusive(alphabet.length)];
      return s.slice(0, i) + ch + s.slice(i);
    }
    case 'replace': {
      const i = rng.nextIntExclusive(s.length);
      const ch = alphabet[rng.nextIntExclusive(alphabet.length)];
      return s.slice(0, i) + ch + s.slice(i+1);
    }
  }
}

function maybe(dist: number, rng: SplitMix64): boolean {
  return rng.nextFloat() < dist;
}

// ---------- Pools (example defaults) ----------
export const defaultPools: Pools = {
  firstNames: { values: ['Анна','Мария','Иван','Алексей','София','Дмитрий','Елена','Сергей','Павел','Ольга'], weights: [8,7,7,6,6,6,5,5,4,4] },
  lastNames:  { values: ['Иванов','Петров','Сидоров','Смирнов','Кузнецов','Попов','Соколов','Лебедев','Семенов','Козлов'] },
  cities:     { values: ['Москва','Санкт-Петербург','Новосибирск','Екатеринбург','Казань','Минск','Алматы'] },
  channels:   { values: ['web','mobile','offline','callcenter'] },
  pos:        { values: ['store-001','store-002','kiosk-01','partner-az'] }
};

// ---------- Profile & Record generation ----------
function clamp01(x: number): number { return Math.max(0, Math.min(1, x)); }

export const defaultConfig: GeneratorConfig = {
  profileSpaceSize: 10n ** 12n,
  buckets: [
    { weight: 90, repeatMultiplier: 1 },
    { weight: 8,  repeatMultiplier: 3 },
    { weight: 2,  repeatMultiplier: 10 },
  ],
  distortions: { swapFirstLast: 0.03, transliterate: 0.08, typo: 0.05 },
  dateSpread: { start: new Date('2024-01-01T00:00:00Z'), end: new Date('2026-01-01T00:00:00Z') },
  pools: defaultPools,
};

function profileIdForIndex(idx: bigint, cfg: GeneratorConfig): bigint {
  const h = fnv1a64(idx);
  return h % cfg.profileSpaceSize;
}

export function buildProfile(profileId: bigint, cfg: GeneratorConfig): Profile {
  const seed = fnv1a64('profile:' + profileId.toString());
  const rng = new SplitMix64(seed);

  const firstName = weightedPick(rng, cfg.pools.firstNames) ?? '';
  const lastName  = weightedPick(rng, cfg.pools.lastNames) ?? '';
  const locale = rng.nextFloat() < 0.3 ? 'en' : 'ru';

  const phonesCount = rng.nextIntInclusive(3);
  const emailsCount = rng.nextIntInclusive(5);
  const loginsCount = rng.nextIntInclusive(2);

  const phones: string[] = [];
  for (let i = 0; i < phonesCount; i++) {
    const base = (rng.nextUint64() % 1_000_000_000n).toString().padStart(9, '0');
    phones.push(`+7${base}`);
  }

  function emailFrom(seedLocal: bigint, i: number): string {
    const r = new SplitMix64(seedLocal + BigInt(i));
    const local = (firstName + '.' + lastName).toLowerCase().replace(/[^a-zа-яё.\-]/gi,'');
    const salt = (r.nextUint64() % 10_000n).toString();
    const domains = ['gmail.com','mail.ru','yahoo.com','outlook.com','yandex.ru'];
    return `${local}${salt}@${domains[r.nextIntExclusive(domains.length)]}`;
  }

  const emails: string[] = [];
  const mailSeed = fnv1a64('email:' + profileId.toString());
  for (let i = 0; i < emailsCount; i++) emails.push(emailFrom(mailSeed, i));

  const logins: string[] = [];
  for (let i = 0; i < loginsCount; i++) {
    const num = (rng.nextUint64() % 10000n).toString().padStart(4,'0');
    const base = (firstName[0] ?? 'u') + lastName;
    logins.push((base + num).toLowerCase());
  }

  return { profileId, firstName, lastName, phones, emails, logins, locale };
}

function distortFields(profile: Profile, variantIndex: number, cfg: GeneratorConfig, recordSeed: bigint) {
  const rng = new SplitMix64(recordSeed + BigInt(variantIndex));

  let firstName = profile.firstName;
  let lastName  = profile.lastName;

  if (maybe(clamp01(cfg.distortions.swapFirstLast), rng)) {
    [firstName, lastName] = [lastName, firstName];
  }
  if (maybe(clamp01(cfg.distortions.transliterate), rng)) {
    firstName = transliterateCyrillicToLatin(firstName);
    lastName  = transliterateCyrillicToLatin(lastName);
  }
  if (maybe(clamp01(cfg.distortions.typo), rng)) firstName = randomTypo(rng, firstName);
  if (maybe(clamp01(cfg.distortions.typo), rng)) lastName  = randomTypo(rng, lastName);


  const email = flatPick(rng, profile.emails) ?? '';
  const phone = flatPick(rng, profile.phones) ?? '';
  const login = flatPick(rng, profile.logins) ?? '';

  return { firstName, lastName, email, phone, login };
}

function timestampForIndex(idx: bigint, cfg: GeneratorConfig): string {
  const startMs = BigInt(cfg.dateSpread.start.getTime());
  const endMs   = BigInt(cfg.dateSpread.end.getTime());
  const span = endMs - startMs;
  const h = fnv1a64('time:' + idx.toString());
  const offset = h % span;
  const ms = startMs + offset;
  return new Date(Number(ms)).toISOString();
}

function amountForIndex(idx: bigint): number {
  const h = fnv1a64('amt:' + idx.toString());
  const rng = new SplitMix64(h);
  return 100 + Math.round(rng.nextFloat() * 10_000) * 100; // 100..10_000 in cents
}

function nonProfileFields(rng: SplitMix64, idx: bigint, cfg: GeneratorConfig) {
  return {
    city: weightedPick(rng, cfg.pools.cities) ?? '',
    channel: weightedPick(rng, cfg.pools.channels) ?? '',
    pos: weightedPick(rng, cfg.pools.pos) ?? '',
  };
}

// ---------- Public API: IdempotentGenerator ----------
export class IdempotentGenerator {
  constructor(public cfg: GeneratorConfig) {}

  profileById(profileId: bigint): Profile {
    return buildProfile(profileId, this.cfg);
  }

  recordByIndex(idx: bigint): RawRecord {
    const profileId = profileIdForIndex(idx, this.cfg);
    const bucket = classifyBucket(profileId, this.cfg.buckets);
    const rawSeed = fnv1a64('raw:' + idx.toString());
    const rawRng = new SplitMix64(rawSeed);
    const variantIndex = rawRng.nextIntInclusive(10_000);
    const profile = buildProfile(profileId, this.cfg);
    const distort = distortFields(profile, variantIndex, this.cfg, fnv1a64('rec:' + idx.toString()));
    const np = nonProfileFields(rawRng, idx, this.cfg);

    return {
      recordIndex: idx,
      profileId,
      variantIndex,
      firstName: distort.firstName,
      lastName: distort.lastName,
      email: distort.email,
      phone: distort.phone,
      login: distort.login,
      pointOfSale: np.pos,
      city: np.city,
      channel: np.channel,
      amount: amountForIndex(idx),
      timestamp: timestampForIndex(idx, this.cfg),
    };
  }

  *iterate(startInclusive: bigint, count: bigint): Generator<RawRecord, void, unknown> {
    let idx = startInclusive;
    for (let i = 0n; i < count; i++) {
      yield this.recordByIndex(idx);
      idx++;
    }
  }
}

// Simple main execution check for tsx
const start = Date.now();
const gen = new IdempotentGenerator(defaultConfig);
const count = 1_000_000n;
const estimateCount = 1_000_000_000n;
[...gen.iterate(1n, count)];
const end = Date.now();
const tookSec = Math.ceil((end - start) / 1000);
console.log(`Time taken for (${count.toLocaleString()}): ${tookSec}s, estimated for (${estimateCount.toLocaleString()}): ${(estimateCount / count) * BigInt(tookSec) / 60n / 60n}h`);
console.log(JSON.stringify([...gen.iterate(1n, 3n)], null, 2));
