param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("up", "etl", "analysis", "train", "stream", "producer", "notebooks", "down")]
    [string]$Task,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$env:PYTHONPATH = Join-Path $PSScriptRoot "src"

function docker { if ($env:DOCKER_WSL) { wsl --cd $PSScriptRoot docker @args } else { docker.exe @args } }

function Submit-SparkJob([string]$Job) {
    docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 2g --executor-memory 3g "/opt/project/src/mushroom/$Job.py"
}

switch ($Task) {
    "up"        { docker compose up -d --build kafka spark-master spark-worker backend }
    "etl"       { Submit-SparkJob "etl" }
    "analysis"  { Submit-SparkJob "analysis" }
    "train"     { Submit-SparkJob "train" }
    "stream"    { docker compose --profile streaming up -d spark-streaming; docker compose logs -f spark-streaming }
    "producer"  { python -m mushroom.producer @Rest }
    "notebooks" { Push-Location notebooks; python -m nbconvert --to notebook --execute --inplace 01_eda.ipynb 02_wyniki.ipynb; Pop-Location }
    "down"      { docker compose --profile streaming down }
}
