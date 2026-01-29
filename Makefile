.PHONY: test bench cover race clean run build stress bench-cache bench-quick

# Запуск main.go
run:
	go run .

# Сборка
build:
	go build -o bin/merkletree-example .

# Быстрые тесты (пропускает долгие)
test:
	go test ./merkletree -v -short

# Все тесты (включая долгие)
test-all:
	go test ./... -v -timeout=60m

# Стресс-тест (только он)
stress:
	go test ./merkletree -v -run TestManagerStressTest -timeout=60m

# Стресс-тест с конкретным количеством деревьев
stress-10:
	go test ./merkletree -v -run "TestManagerStressTest/Trees_10" -timeout=10m

stress-50:
	go test ./merkletree -v -run "TestManagerStressTest/Trees_50" -timeout=20m

stress-100:
	go test ./merkletree -v -run "TestManagerStressTest/Trees_100" -timeout=30m

# Быстрые бенчмарки (без длинных тестов)
bench:
	go test ./merkletree -bench=. -benchmem -short -run=^$

# Только бенчмарки кеша
bench-cache:
	go test ./merkletree -bench=BenchmarkCache -benchmem -short -run=^$$

# Быстрый бенчмарк (короткое время)
bench-quick:
	go test ./merkletree -bench=. -benchtime=1s -short -run=^$$

# Бенчмарки масштабирования
bench-scaling:
	go test ./merkletree -bench=BenchmarkManagerScaling -benchmem -short -run=^$$

# Бенчмарки глобального корня
bench-root:
	go test ./merkletree -bench=BenchmarkGlobalRootComputation -benchmem -short -run=^$$

# Параллельные бенчмарки
bench-parallel:
	go test ./merkletree -bench=BenchmarkParallelRootComputation -benchmem -short -run=^$$

# Покрытие
cover:
	go test ./merkletree -short -coverprofile=coverage.out
	go tool cover -html=coverage.out

# Race detector
race:
	go test ./merkletree -v -race -short

# Профилирование CPU
profile-cpu:
	go test ./merkletree -bench=BenchmarkTreeInsert -cpuprofile=cpu.prof -short -run=^$$
	@echo "Запустите: go tool pprof -http=:8080 cpu.prof"

# Профилирование памяти
profile-mem:
	go test ./merkletree -bench=BenchmarkTreeGet -memprofile=mem.prof -short -run=^$$
	@echo "Запустите: go tool pprof -http=:8080 mem.prof"

# Профилирование кеша
profile-cache:
	go test ./merkletree -bench=BenchmarkCacheHit -memprofile=cache_mem.prof -short -run=^$$
	@echo "Запустите: go tool pprof -http=:8080 cache_mem.prof"

# Сравнение стратегий обновления
bench-updates:
	go test ./merkletree -bench=BenchmarkUpdateStrategies -benchmem -benchtime=3s

# Lazy vs Eager hashing
bench-hashing:
	go test ./merkletree -bench=BenchmarkLazyVsEagerHashing -benchmem

# Overhead анализ
bench-overhead:
	go test ./merkletree -bench=BenchmarkHashingOverhead -benchmem

# Реальный сценарий
bench-realworld:
	go test ./merkletree -bench=BenchmarkRealWorldScenario -benchmem -benchtime=5s

# Все оптимизационные бенчмарки
bench-opt:
	go test ./merkletree -bench=Benchmark.*Strateg -benchmem -benchtime=3s

# Сравнение менеджера
bench-manager-opt:
	go test ./merkletree -bench=BenchmarkManagerUpdateStrategies -benchmem

# Полный отчет
bench-compare:
	@echo "=== Regular vs Optimized ==="
	@go test ./merkletree -bench=BenchmarkUpdateStrategies/Updates_10000 -benchmem -benchtime=3s
	@echo ""
	@echo "=== Lazy Hashing ==="
	@go test ./merkletree -bench=BenchmarkLazyVsEagerHashing -benchmem
	@echo ""
	@echo "=== Real World ==="
	@go test ./merkletree -bench=BenchmarkRealWorldScenario -benchmem -benchtime=5s

# Очистка
clean:
	rm -f coverage.out *.prof
	rm -rf bin/

# Быстрая проверка
quick:
	go test ./merkletree -v -short -race

# Всё сразу (быстрые тесты)
all: deps fmt test bench-quick

# Форматирование
fmt:
	go fmt ./...

# Обновление зависимостей
deps:
	go mod download
	go mod tidy
