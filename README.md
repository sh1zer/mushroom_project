# Big Data: klasyfikacja jadalności grzybów (Secondary Mushroom Dataset)

Projekt z przedmiotu Big Data. Dane: [UCI Secondary Mushroom Dataset (id 848)](https://archive.ics.uci.edu/dataset/848/secondary+mushroom+dataset).
Pełny opis (cel, tezy, dane, ETL, ML, wnioski, źródła) znajduje się w [docs/sprawozdanie.md](docs/sprawozdanie.md).

## Architektura

```
Wsadowo:     data/raw/*.csv ──► etl.py ──► data/processed/mushrooms.parquet ──► analysis.py, train.py ──► reports/*.json, models/best_model

Strumieniowo: producer.py ─┐
              formularz WWW ┴─► Kafka: mushroom-observations ──► stream_job.py (Spark + model MLlib)
                                                                   ├──► Kafka: mushroom-predictions ──► FastAPI ──► przeglądarka
                                                                   └──► konsola: liczba decyzji na minutę (okno czasowe)
```

| Warstwa | Technologia | Pliki |
|---|---|---|
| Pozyskanie danych | UCI, `urllib` / `ucimlrepo` | `scripts/download_data.py` |
| EDA | Jupyter, pandas, seaborn, scipy | `notebooks/01_eda.ipynb` |
| ETL | Spark SQL / DataFrame API → Parquet | `src/mushroom/etl.py`, `transforms.py` |
| Analiza statystyczna | Spark MLlib: `ChiSquareTest`, `FPGrowth` | `src/mushroom/analysis.py` |
| Uczenie maszynowe | Spark MLlib: 4 klasyfikatory, `CrossValidator` z foldami po gatunkach | `src/mushroom/train.py` |
| Strumień | Apache Kafka (KRaft), Spark Structured Streaming | `src/mushroom/producer.py`, `stream_job.py` |
| Backend | FastAPI, kafka-python | `backend/main.py` |
| Frontend | HTML + JS | `frontend/` |
| Wyniki | Jupyter | `notebooks/02_wyniki.ipynb`, `reports/` |
| Infrastruktura | Docker Compose: Kafka, Spark master + worker, backend | `docker-compose.yml`, `docker/` |

## Wymagania

- Docker Desktop (Kafka i klaster Spark działają w kontenerach; na Windows Spark nie zapisuje plików bez `winutils`, więc zadania Spark uruchamiamy w Linuksowych kontenerach),
- Python 3.11 i `pip install -r requirements.txt` (producent, backend, notatniki),
- PyCharm (Professional pozwala ustawić interpreter z usługi `spark-master` w Docker Compose; w Community uruchamiamy zadania przez `run.ps1`).

## Uruchomienie krok po kroku (PowerShell, w katalogu projektu)

```powershell
pip install -r requirements.txt
```

```powershell
.\run.ps1 download
```

```powershell
.\run.ps1 up
```

Po starcie: Spark Master UI http://localhost:8080, dashboard http://localhost:8000.

```powershell
.\run.ps1 etl
```

```powershell
.\run.ps1 analysis
```

```powershell
.\run.ps1 train
```

`train` trwa kilka minut (4 modele x 2 scenariusze + strojenie lasu losowego w 4-krotnej walidacji krzyżowej).

```powershell
.\run.ps1 stream
```

W drugim terminalu (1000 zdarzeń do Kafki, 5 zdarzeń/s):

```powershell
.\run.ps1 producer --rate 5
```

Dashboard http://localhost:8000 pokazuje wtedy ostatnie predykcje i pozwala wysłać własną obserwację przez formularz (droga: przeglądarka → FastAPI → Kafka → Spark → Kafka → FastAPI → przeglądarka). Logi zadania strumieniowego pokazują co minutę liczbę decyzji w oknie czasowym.

Na koniec odświeżenie notatników z wynikami:

```powershell
.\run.ps1 notebooks
```

## Najważniejsze wyniki

| | Gatunki znane (losowy podział) | Gatunki niewidziane w treningu |
|---|---|---|
| Najlepsza accuracy | 99,1% (RandomForest, GBT) | 64,9% (GBT), przy bazie 55,6% |
| AUC wdrożonego RandomForest | 0,9997 | 0,583 |
| Próg bezpieczeństwa 0,4: czułość / swoistość | 100% / 95,1% | 80,1% / 26,5% |

Model dobrze rozpoznaje gatunki, które zna, ale nie przenosi tej wiedzy na nowe. Dlatego w strumieniu służy do ostrzegania przed trującymi grzybami (próg 0,4). Szczegóły są w sprawozdaniu.

## Uruchamianie z PyCharm

1. Otwórz katalog `mushroom_project` jako projekt, oznacz `src` jako *Sources Root*.
2. Zadania Spark: interpreter *Docker Compose* → usługa `spark-master` (ścieżka projektu w kontenerze: `/opt/project`), konfiguracje uruchomieniowe dla `src/mushroom/etl.py`, `analysis.py`, `train.py`. Zmienna `SPARK_MASTER=spark://spark-master:7077` jest już ustawiona w kontenerze.
3. Producent: lokalny interpreter Python 3.11, moduł `mushroom.producer` (zmienna `PYTHONPATH=src`).

## Struktura katalogów

```
mushroom_project/
├── backend/main.py            API FastAPI + konsument Kafki
├── frontend/                  strona (index.html, app.js, styles.css)
├── src/mushroom/
│   ├── config.py              ścieżki, lista cech, słownik kodów, tematy Kafki, próg bezpieczeństwa
│   ├── transforms.py          wspólne czyszczenie danych (batch i streaming)
│   ├── etl.py                 CSV → Parquet
│   ├── analysis.py            test χ², FP-Growth
│   ├── train.py               porównanie, strojenie i zapis modelu
│   ├── producer.py            symulator strumienia → Kafka
│   └── stream_job.py          Spark Structured Streaming: Kafka → model → Kafka
├── notebooks/                 01_eda.ipynb, 02_wyniki.ipynb
├── reports/                   JSON z wynikami i wykresy (figures/)
├── docs/sprawozdanie.md       sprawozdanie
├── docker/                    obrazy Spark i backendu
├── docker-compose.yml
└── run.ps1                    skróty do uruchamiania zadań
```
