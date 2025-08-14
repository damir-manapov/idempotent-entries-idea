package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
)

// Types
type FrequencyBucket struct {
	Weight           int `json:"weight"`
	RepeatMultiplier int `json:"repeatMultiplier"`
}

type DistortionRates struct {
	SwapFirstLast float64 `json:"swapFirstLast"`
	Transliterate float64 `json:"transliterate"`
	Typo          float64 `json:"typo"`
}

type DateSpreadConfig struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type GeneratorConfig struct {
	ProfileSpaceSize uint64            `json:"profileSpaceSize"`
	Buckets          []FrequencyBucket `json:"buckets"`
	Distortions      DistortionRates   `json:"distortions"`
	DateSpread       DateSpreadConfig  `json:"dateSpread"`
	Pools            Pools             `json:"pools"`
}

type Profile struct {
	ProfileID  uint64   `json:"profileId"`
	FirstName  string   `json:"firstName"`
	LastName   string   `json:"lastName"`
	Phones     []string `json:"phones"`
	Emails     []string `json:"emails"`
	Logins     []string `json:"logins"`
	Locale     string   `json:"locale"`
}

type RawRecord struct {
	RecordIndex   uint64  `json:"recordIndex"`
	ProfileID     uint64  `json:"profileId"`
	VariantIndex  int     `json:"variantIndex"`
	FirstName     string  `json:"firstName"`
	LastName      string  `json:"lastName"`
	Email         string  `json:"email"`
	Phone         string  `json:"phone"`
	Login         string  `json:"login"`
	PointOfSale   string  `json:"pointOfSale"`
	City          string  `json:"city"`
	Channel       string  `json:"channel"`
	Amount        float64 `json:"amount"`
	Timestamp     string  `json:"timestamp"`
}

type Pools struct {
	FirstNames []string  `json:"firstNames"`
	LastNames  []string  `json:"lastNames"`
	Cities     []string  `json:"cities"`
	Channels   []string  `json:"channels"`
	POS        []string  `json:"pos"`
}

// Utilities: 64-bit hashing & PRNG
func fnv1a64(input interface{}) uint64 {
	var data []byte
	switch v := input.(type) {
	case uint64:
		data = make([]byte, 8)
		binary.LittleEndian.PutUint64(data, v)
	case string:
		data = []byte(v)
	default:
		return 0
	}

	hash := uint64(0xcbf29ce484222325)
	prime := uint64(0x100000001b3)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}

type SplitMix64 struct {
	state uint64
}

func NewSplitMix64(seed uint64) *SplitMix64 {
	return &SplitMix64{state: seed}
}

func (s *SplitMix64) NextUint64() uint64 {
	z := s.state + 0x9E3779B97F4A7C15
	s.state = z
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (s *SplitMix64) NextFloat() float64 {
	u := s.NextUint64()
	return float64(u>>11) / float64(1<<53)
}

func (s *SplitMix64) NextInt(maxExclusive int) int {
	return int(s.NextFloat() * float64(maxExclusive))
}

func weightedPick(rng *SplitMix64, values []string, weights []int) string {
	if len(weights) == 0 {
		return values[rng.NextInt(len(values))]
	}

	total := 0
	for _, w := range weights {
		total += w
	}

	r := rng.NextFloat() * float64(total)
	for i, w := range weights {
		r -= float64(w)
		if r <= 0 {
			return values[i]
		}
	}
	return values[len(values)-1]
}

func classifyBucket(profileID uint64, buckets []FrequencyBucket) FrequencyBucket {
	seed := fnv1a64(profileID)
	rng := NewSplitMix64(seed)
	
	total := 0
	for _, b := range buckets {
		total += b.Weight
	}

	r := rng.NextFloat() * float64(total)
	for _, b := range buckets {
		r -= float64(b.Weight)
		if r <= 0 {
			return b
		}
	}
	return buckets[len(buckets)-1]
}

// Distortions
var cyrToLatMap = map[rune]string{
	'А': "A", 'Б': "B", 'В': "V", 'Г': "G", 'Д': "D", 'Е': "E", 'Ё': "E", 'Ж': "Zh",
	'З': "Z", 'И': "I", 'Й': "Y", 'К': "K", 'Л': "L", 'М': "M", 'Н': "N", 'О': "O",
	'П': "P", 'Р': "R", 'С': "S", 'Т': "T", 'У': "U", 'Ф': "F", 'Х': "Kh", 'Ц': "Ts",
	'Ч': "Ch", 'Ш': "Sh", 'Щ': "Sch", 'Ъ': "", 'Ы': "Y", 'Ь': "", 'Э': "E", 'Ю': "Yu", 'Я': "Ya",
	'а': "a", 'б': "b", 'в': "v", 'г': "g", 'д': "d", 'е': "e", 'ё': "e", 'ж': "zh",
	'з': "z", 'и': "i", 'й': "y", 'к': "k", 'л': "l", 'м': "m", 'н': "n", 'о': "o",
	'п': "p", 'р': "r", 'с': "s", 'т': "t", 'у': "u", 'ф': "f", 'х': "kh", 'ц': "ts",
	'ч': "ch", 'ш': "sh", 'щ': "sch", 'ъ': "", 'ы': "y", 'ь': "", 'э': "e", 'ю': "yu", 'я': "ya",
}

func transliterateCyrillicToLatin(s string) string {
	result := ""
	for _, ch := range s {
		if lat, ok := cyrToLatMap[ch]; ok {
			result += lat
		} else {
			result += string(ch)
		}
	}
	return result
}

func randomTypo(rng *SplitMix64, s string) string {
	if len(s) == 0 {
		return s
	}

	// For very short strings, just return as-is to avoid complications
	if len(s) <= 2 {
		return s
	}

	ops := []string{"delete", "insert", "replace"} // Removed swap for safety
	op := ops[rng.NextInt(len(ops))]
	alphabet := "abcdefghijklmnopqrstuvwxyz"

	switch op {
	case "delete":
		i := rng.NextInt(len(s))
		if i >= len(s) {
			i = len(s) - 1
		}
		return s[:i] + s[i+1:]
	case "insert":
		i := rng.NextInt(len(s) + 1)
		if i > len(s) {
			i = len(s)
		}
		ch := alphabet[rng.NextInt(len(alphabet))]
		return s[:i] + string(ch) + s[i:]
	case "replace":
		i := rng.NextInt(len(s))
		if i >= len(s) {
			i = len(s) - 1
		}
		ch := alphabet[rng.NextInt(len(alphabet))]
		return s[:i] + string(ch) + s[i+1:]
	}
	return s
}

func maybe(dist float64, rng *SplitMix64) bool {
	return rng.NextFloat() < dist
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

// Pools (example defaults)
var defaultPools = Pools{
	FirstNames: []string{"Анна", "Мария", "Иван", "Алексей", "София", "Дмитрий", "Елена", "Сергей", "Павел", "Ольга"},
	LastNames:  []string{"Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Соколов", "Лебедев", "Семенов", "Козлов"},
	Cities:     []string{"Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань", "Минск", "Алматы"},
	Channels:   []string{"web", "mobile", "offline", "callcenter"},
	POS:        []string{"store-001", "store-002", "kiosk-01", "partner-az"},
}

var defaultConfig = GeneratorConfig{
	ProfileSpaceSize: 1000000000000, // 10^12
	Buckets: []FrequencyBucket{
		{Weight: 90, RepeatMultiplier: 1},
		{Weight: 8, RepeatMultiplier: 3},
		{Weight: 2, RepeatMultiplier: 10},
	},
	Distortions: DistortionRates{
		SwapFirstLast: 0.03,
		Transliterate: 0.08,
		Typo:          0.05,
	},
	DateSpread: DateSpreadConfig{
		Start: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	},
	Pools: defaultPools,
}

func profileIDForIndex(idx uint64, cfg GeneratorConfig) uint64 {
	h := fnv1a64(idx)
	return h % cfg.ProfileSpaceSize
}

func variantForIndex(idx uint64, multiplier int) int {
	if multiplier <= 1 {
		return 0
	}
	h := fnv1a64(idx ^ 0xA5A5A5A5A5A5A5A5)
	rng := NewSplitMix64(h)
	return rng.NextInt(multiplier)
}

func buildProfile(profileID uint64, cfg GeneratorConfig) Profile {
	seed := fnv1a64("profile:" + fmt.Sprintf("%d", profileID))
	rng := NewSplitMix64(seed)

	firstName := weightedPick(rng, cfg.Pools.FirstNames, []int{8, 7, 7, 6, 6, 6, 5, 5, 4, 4})
	lastName := weightedPick(rng, cfg.Pools.LastNames, nil)
	locale := "ru"
	if rng.NextFloat() < 0.3 {
		locale = "en"
	}

	phonesCount := 1 + rng.NextInt(3)
	emailsCount := 1 + rng.NextInt(5)
	loginsCount := 1 + rng.NextInt(2)

	phones := make([]string, phonesCount)
	for i := 0; i < phonesCount; i++ {
		base := fmt.Sprintf("%09d", rng.NextUint64()%1000000000)
		cc := "+7"
		if rng.NextFloat() < 0.5 {
			cc = "+48"
		}
		phones[i] = cc + base
	}

	emails := make([]string, emailsCount)
	mailSeed := fnv1a64("email:" + fmt.Sprintf("%d", profileID))
	for i := 0; i < emailsCount; i++ {
		r := NewSplitMix64(mailSeed + uint64(i))
		local := fmt.Sprintf("%s.%s", firstName, lastName)
		local = strings.ToLower(local)
		// Simple regex replacement for non-alphanumeric chars
		for _, ch := range local {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'а' && ch <= 'я') || ch == '.' || ch == '-') {
				local = strings.ReplaceAll(local, string(ch), "")
			}
		}
		salt := fmt.Sprintf("%04d", r.NextUint64()%10000)
		domains := []string{"gmail.com", "mail.ru", "yahoo.com", "outlook.com", "yandex.ru"}
		emails[i] = local + salt + "@" + domains[r.NextInt(len(domains))]
	}

	logins := make([]string, loginsCount)
	for i := 0; i < loginsCount; i++ {
		num := fmt.Sprintf("%04d", rng.NextUint64()%10000)
		base := ""
		if len(firstName) > 0 {
			base = string([]rune(firstName)[0]) + lastName
		} else {
			base = "u" + lastName
		}
		logins[i] = strings.ToLower(base + num)
	}

	return Profile{
		ProfileID: profileID,
		FirstName: firstName,
		LastName:  lastName,
		Phones:    phones,
		Emails:    emails,
		Logins:    logins,
		Locale:    locale,
	}
}

func distortFields(profile Profile, variantIndex int, cfg GeneratorConfig, recordSeed uint64) (string, string, string, string, string) {
	rng := NewSplitMix64(recordSeed + uint64(variantIndex))

	firstName := profile.FirstName
	lastName := profile.LastName

	if maybe(clamp01(cfg.Distortions.SwapFirstLast), rng) {
		firstName, lastName = lastName, firstName
	}
	if maybe(clamp01(cfg.Distortions.Transliterate), rng) {
		firstName = transliterateCyrillicToLatin(firstName)
		lastName = transliterateCyrillicToLatin(lastName)
	}
	if maybe(clamp01(cfg.Distortions.Typo), rng) {
		firstName = randomTypo(rng, firstName)
	}
	if maybe(clamp01(cfg.Distortions.Typo), rng) {
		lastName = randomTypo(rng, lastName)
	}

	// Safe array access with fallbacks
	var email, phone, login string
	
	if len(profile.Emails) > 0 {
		email = profile.Emails[rng.NextInt(len(profile.Emails))]
	} else {
		email = "default@example.com"
	}
	
	if len(profile.Phones) > 0 {
		phone = profile.Phones[rng.NextInt(len(profile.Phones))]
	} else {
		phone = "+7000000000"
	}
	
	if len(profile.Logins) > 0 {
		login = profile.Logins[rng.NextInt(len(profile.Logins))]
	} else {
		login = "defaultuser"
	}

	return firstName, lastName, email, phone, login
}

func timestampForIndex(idx uint64, cfg GeneratorConfig) string {
	startMs := uint64(cfg.DateSpread.Start.UnixMilli())
	endMs := uint64(cfg.DateSpread.End.UnixMilli())
	span := endMs - startMs
	h := fnv1a64("time:" + fmt.Sprintf("%d", idx))
	offset := h % span
	ms := startMs + offset
	return time.UnixMilli(int64(ms)).UTC().Format(time.RFC3339)
}

func amountForIndex(idx uint64) float64 {
	h := fnv1a64("amt:" + fmt.Sprintf("%d", idx))
	rng := NewSplitMix64(h)
	sum := 0.0
	for i := 0; i < 12; i++ {
		sum += rng.NextFloat()
	}
	normal := sum - 6.0
	base := math.Exp(normal*0.35 + 3)
	return math.Round(base*100) / 100
}

func nonProfileFields(idx uint64, cfg GeneratorConfig) (string, string, string) {
	h := fnv1a64("np:" + fmt.Sprintf("%d", idx))
	rng := NewSplitMix64(h)
	
	city := weightedPick(rng, cfg.Pools.Cities, nil)
	channel := weightedPick(rng, cfg.Pools.Channels, nil)
	pos := weightedPick(rng, cfg.Pools.POS, nil)
	
	return city, channel, pos
}

// Public API: IdempotentGenerator
type IdempotentGenerator struct {
	cfg GeneratorConfig
}

func NewIdempotentGenerator(cfg GeneratorConfig) *IdempotentGenerator {
	return &IdempotentGenerator{cfg: cfg}
}

func (g *IdempotentGenerator) ProfileByID(profileID uint64) Profile {
	return buildProfile(profileID, g.cfg)
}

func (g *IdempotentGenerator) RecordByIndex(idx uint64) RawRecord {
	profileID := profileIDForIndex(idx, g.cfg)
	bucket := classifyBucket(profileID, g.cfg.Buckets)
	variantIndex := variantForIndex(idx, bucket.RepeatMultiplier)
	profile := buildProfile(profileID, g.cfg)
	firstName, lastName, email, phone, login := distortFields(profile, variantIndex, g.cfg, fnv1a64("rec:"+fmt.Sprintf("%d", idx)))
	city, channel, pos := nonProfileFields(idx, g.cfg)

	return RawRecord{
		RecordIndex:   idx,
		ProfileID:     profileID,
		VariantIndex:  variantIndex,
		FirstName:     firstName,
		LastName:      lastName,
		Email:         email,
		Phone:         phone,
		Login:         login,
		PointOfSale:   pos,
		City:          city,
		Channel:       channel,
		Amount:        amountForIndex(idx),
		Timestamp:     timestampForIndex(idx, g.cfg),
	}
}

func (g *IdempotentGenerator) Iterate(startInclusive, count uint64) []RawRecord {
	records := make([]RawRecord, count)
	for i := uint64(0); i < count; i++ {
		records[i] = g.RecordByIndex(startInclusive + i)
	}
	return records
}

func main() {
	gen := NewIdempotentGenerator(defaultConfig)
	
	// Performance benchmark: generate 1M records WITH saving
	fmt.Println("🚀 Performance Benchmark: Generating and Saving 1,000,000 records...")
	
	// Create output directory
	os.MkdirAll("output", 0755)
	
	// Generate and save 1M records
	start := time.Now()
	
	// Open file for writing
	file, err := os.Create("output/records_1m.jsonl")
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()
	
	// Generate 1M records and save them line by line (JSONL format for efficiency)
	recordsGenerated := 0
	for i := uint64(0); i < 1_000_000; i++ {
		record := gen.RecordByIndex(i)
		
		// Convert to JSON
		jsonData, err := json.Marshal(record)
		if err != nil {
			fmt.Printf("Error marshaling record %d: %v\n", i, err)
			continue
		}
		
		// Write to file with newline
		_, err = file.Write(append(jsonData, '\n'))
		if err != nil {
			fmt.Printf("Error writing record %d: %v\n", i, err)
			continue
		}
		
		recordsGenerated++
		
		// Progress indicator every 100K records
		if recordsGenerated%100_000 == 0 {
			fmt.Printf("📝 Generated and saved %d records...\n", recordsGenerated)
		}
	}
	
	// Ensure all data is written to disk
	file.Sync()
	
	totalDuration := time.Since(start)
	recordsPerSecond := float64(recordsGenerated) / totalDuration.Seconds()
	
	fmt.Printf("✅ Generated and saved %d records in %v\n", recordsGenerated, totalDuration)
	fmt.Printf("📊 Speed: %.0f records/second (generation + I/O)\n", recordsPerSecond)
	fmt.Printf("⏱️  Average: %.3f microseconds per record\n", float64(totalDuration.Microseconds())/float64(recordsGenerated))
	
	// Get file size
	fileInfo, err := file.Stat()
	if err == nil {
		fileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)
		fmt.Printf("💾 File size: %.2f MB\n", fileSizeMB)
		fmt.Printf("📊 Data rate: %.2f MB/s\n", fileSizeMB/totalDuration.Seconds())
	}
	
	// Estimate time for 1 billion records with I/O
	fmt.Println("\n🔮 Time Estimation for 1 Billion Records (with I/O):")
	billionRecords := 1_000_000_000
	estimatedSeconds := float64(billionRecords) / recordsPerSecond
	estimatedDuration := time.Duration(estimatedSeconds * float64(time.Second))
	
	fmt.Printf("📈 Target: 1,000,000,000 records\n")
	fmt.Printf("⏱️  Estimated time: %v\n", estimatedDuration)
	fmt.Printf("🕐 Estimated time (human readable): %s\n", formatDuration(estimatedDuration))
	
	// Estimate storage requirements
	if fileInfo != nil {
		estimatedSizeGB := float64(fileInfo.Size()) * float64(billionRecords) / float64(recordsGenerated) / (1024 * 1024 * 1024)
		fmt.Printf("💾 Estimated storage: %.2f GB\n", estimatedSizeGB)
	}
	
	// Now generate a small sample for display
	fmt.Println("\n📋 Sample Output (5 records):")
	sample := gen.Iterate(0, 5)
	
	// Convert to JSON
	jsonData, err := json.MarshalIndent(sample, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling JSON: %v\n", err)
		return
	}
	
	fmt.Println(string(jsonData))
}

// Helper function to format duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d.Hours() >= 24 {
		days := int(d.Hours() / 24)
		hours := int(d.Hours()) % 24
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%d days, %d hours, %d minutes", days, hours, minutes)
	} else if d.Hours() >= 1 {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%d hours, %d minutes", hours, minutes)
	} else if d.Minutes() >= 1 {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%d minutes, %d seconds", minutes, seconds)
	} else {
		return fmt.Sprintf("%.2f seconds", d.Seconds())
	}
}

// # Run the Go code
// go run main.go

// # Or build and run
// go build -o generator main.go
// ./generator
