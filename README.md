# Grzyby - projekt Big Data

Sprawozdanie: `docs/sprawozdanie.md`

## Instalacja

Potrzebny Docker i Python 3.11. Jeśli Docker Desktop nie działa, a Docker jest zainstalowany w Ubuntu (WSL), najpierw w terminalu:

```powershell
$env:DOCKER_WSL = "1"
```

```powershell
pip install -r requirements.txt
.\run.ps1 download
.\run.ps1 up
```

## Zadania Spark

```powershell
.\run.ps1 etl
.\run.ps1 analysis
.\run.ps1 train
```

## Strumień

```powershell
.\run.ps1 stream
```

W drugim terminalu:

```powershell
.\run.ps1 producer
```

Strona: http://localhost:8000, Spark UI: http://localhost:8080

## Notatniki i sprzątanie

```powershell
.\run.ps1 notebooks
.\run.ps1 down
```
