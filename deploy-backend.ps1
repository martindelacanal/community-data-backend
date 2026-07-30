#Requires -Version 5.1

<#
.SYNOPSIS
Deploys origin/main to the Bienestar Community EC2 backend.

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy-backend.ps1

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy-backend.ps1 -DryRun
#>

[CmdletBinding()]
param(
    [switch]$DryRun,
    [switch]$Yes
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$SshHost = 'ec2-54-219-232-247.us-west-1.compute.amazonaws.com'
$SshUser = 'ubuntu'
$SshTarget = "$SshUser@$SshHost"
$KeyPath = Join-Path $env:USERPROFILE 'Desktop\claves-community-data-server.pem'
$ExpectedBranch = 'main'
$PublicPingUrl = 'https://api.bienestarcommunity.org/api/ping'
$PublicDatabaseUrl = 'https://api.bienestarcommunity.org/api/mobile-app/versions'

$script:GitExe = $null
$script:SshExe = $null

function Write-Section {
    param([string]$Message)

    Write-Host ''
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)

    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Resolve-Executable {
    param(
        [string]$Name,
        [string[]]$Candidates = @()
    )

    $command = Get-Command -Name $Name -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1

    if ($command) {
        return $command.Source
    }

    foreach ($candidate in $Candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw "No se encontro '$Name'."
}

function Invoke-NativeText {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$WorkingDirectory,
        [string]$Description = 'Comando externo',
        [switch]$ShowOutput
    )

    $pushed = $false
    $previousErrorActionPreference = $ErrorActionPreference

    try {
        if ($WorkingDirectory) {
            Push-Location -LiteralPath $WorkingDirectory
            $pushed = $true
        }

        # Windows PowerShell 5.1 convierte stderr nativo en ErrorRecord.
        # Git, SSH, npm y PM2 tambien usan stderr para mensajes validos.
        $ErrorActionPreference = 'Continue'
        $output = & $FilePath @Arguments 2>&1
        $exitCode = $LASTEXITCODE
        $text = ($output | ForEach-Object { "$_" }) -join [Environment]::NewLine

        if ($ShowOutput -and -not [string]::IsNullOrWhiteSpace($text)) {
            Write-Host $text
        }

        if ($exitCode -ne 0) {
            if ([string]::IsNullOrWhiteSpace($text)) {
                throw "$Description fallo con codigo $exitCode."
            }

            throw "$Description fallo con codigo $exitCode.`n$text"
        }

        return $text.Trim()
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
        if ($pushed) {
            Pop-Location
        }
    }
}

function Invoke-GitText {
    param(
        [string[]]$Arguments,
        [string]$RepoRoot,
        [string]$Description = 'Git',
        [switch]$ShowOutput
    )

    return Invoke-NativeText -FilePath $script:GitExe -Arguments $Arguments `
        -WorkingDirectory $RepoRoot -Description $Description -ShowOutput:$ShowOutput
}

function Assert-LocalRepository {
    param([string]$RepoRoot)

    if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot '.git') -PathType Container)) {
        throw "No se encontro un repositorio Git en $RepoRoot"
    }

    $branch = Invoke-GitText -Arguments @('branch', '--show-current') `
        -RepoRoot $RepoRoot -Description 'Leer rama local'

    if ($branch -ne $ExpectedBranch) {
        throw "El backend debe estar en la rama '$ExpectedBranch'; actualmente esta en '$branch'."
    }

    Invoke-GitText -Arguments @(
        'fetch',
        'origin',
        '+refs/heads/main:refs/remotes/origin/main'
    ) -RepoRoot $RepoRoot -Description 'Actualizar origin/main' -ShowOutput | Out-Null

    $aheadText = Invoke-GitText -Arguments @(
        'rev-list',
        '--count',
        'refs/remotes/origin/main..refs/heads/main'
    ) -RepoRoot $RepoRoot -Description 'Comprobar commits sin publicar'

    $ahead = 0
    if (-not [int]::TryParse($aheadText, [ref]$ahead)) {
        throw "Git devolvio un conteo inesperado: $aheadText"
    }

    if ($ahead -gt 0) {
        throw "La rama main tiene $ahead commit(s) sin push. Ejecuta git push antes del deploy."
    }

    $statusText = Invoke-GitText -Arguments @(
        'status',
        '--porcelain=v1',
        '--untracked-files=normal'
    ) -RepoRoot $RepoRoot -Description 'Comprobar cambios locales'

    $changes = @(
        $statusText -split '\r?\n' |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $unexpectedChanges = @(
        $changes | Where-Object {
            $_.Length -lt 4 -or $_.Substring(3) -ne 'deploy-backend.ps1'
        }
    )

    if ($unexpectedChanges.Count -gt 0) {
        throw (
            "Hay cambios locales del backend que no estan en GitHub. " +
            "Haz commit y push o deja el repositorio limpio antes del deploy."
        )
    }

    if ($changes.Count -gt 0) {
        Write-Warning 'El unico cambio local es este script nuevo; el codigo desplegado proviene de origin/main.'
    }

    $localCommit = Invoke-GitText -Arguments @('rev-parse', 'refs/heads/main') `
        -RepoRoot $RepoRoot -Description 'Leer commit local'
    $remoteCommit = Invoke-GitText -Arguments @('rev-parse', 'refs/remotes/origin/main') `
        -RepoRoot $RepoRoot -Description 'Leer commit remoto'

    Write-Success "Repositorio local verificado: main $($localCommit.Substring(0, 7))"
    Write-Host "Commit disponible en origin/main: $($remoteCommit.Substring(0, 7))"
}

function Confirm-ProductionDeploy {
    if ($DryRun -or $Yes) {
        return
    }

    Write-Host ''
    Write-Warning "Se actualizara y reiniciara el backend de produccion en $SshHost."
    $answer = Read-Host 'Escribe PRODUCCION para continuar'
    if ($answer -cne 'PRODUCCION') {
        throw [System.OperationCanceledException]::new('Deploy cancelado.')
    }
}

function Get-RemoteBash {
    param([bool]$IsDryRun)

    $dryRunValue = '0'
    if ($IsDryRun) {
        $dryRunValue = '1'
    }

    $bash = @'
set -Eeuo pipefail
umask 077

repo="$HOME/community-data-backend"
branch="main"
app_name="server"
expected_script="$repo/server.js"
expected_origin_https="https://github.com/martindelacanal/community-data-backend.git"
expected_origin_ssh="git@github.com:martindelacanal/community-data-backend.git"
dry_run=__DRY_RUN__

log() {
  printf '[REMOTE] %s\n' "$*"
}

fail() {
  printf '[REMOTE ERROR] %s\n' "$*" >&2
  exit 1
}

on_error() {
  code=$?
  printf '[REMOTE ERROR] Fallo en la linea %s (codigo %s).\n' "$1" "$code" >&2
  exit "$code"
}

trap 'on_error "$LINENO"' ERR

for command_name in git curl flock base64; do
  command -v "$command_name" >/dev/null 2>&1 ||
    fail "No se encontro $command_name en EC2."
done

export NVM_DIR="$HOME/.nvm"
test -s "$NVM_DIR/nvm.sh" || fail "No se encontro NVM en $NVM_DIR."
. "$NVM_DIR/nvm.sh"

for command_name in node npm pm2; do
  command -v "$command_name" >/dev/null 2>&1 ||
    fail "No se encontro $command_name despues de cargar NVM."
done

exec 9>"/tmp/community-data-backend-deploy.lock"
flock -n 9 || fail "Ya hay otro deploy del backend en ejecucion."

test -d "$repo/.git" || fail "No existe el repositorio $repo."
cd "$repo"

for required_file in .env package.json package-lock.json app.js server.js; do
  test -f "$required_file" || fail "Falta $repo/$required_file."
done

current_branch="$(git branch --show-current)"
test "$current_branch" = "$branch" ||
  fail "La rama remota es '$current_branch'; se esperaba '$branch'."

origin_url="$(git remote get-url origin)"
case "$origin_url" in
  "$expected_origin_https"|"$expected_origin_ssh")
    ;;
  *)
    fail "El remote origin de EC2 no es el repositorio esperado."
    ;;
esac

if test -n "$(git status --porcelain=v1 --untracked-files=normal)"; then
  fail "El repositorio remoto contiene cambios locales. No se hizo stash ni reset."
fi

assert_pm2_configuration() {
  pm2 jlist | node -e '
    let input = "";
    process.stdin.on("data", chunk => input += chunk);
    process.stdin.on("end", () => {
      const processes = JSON.parse(input).filter(item => item.name === "server");
      const fail = message => {
        console.error("[REMOTE ERROR] " + message);
        process.exit(1);
      };

      if (processes.length !== 1) {
        fail("PM2 debe contener exactamente un proceso llamado server.");
      }

      const processInfo = processes[0];
      const env = processInfo.pm2_env || {};

      if (env.status !== "online") fail("El proceso PM2 server no esta online.");
      if (env.exec_mode !== "fork_mode") fail("El proceso server no usa fork_mode.");
      if (Number(env.instances) !== 1) fail("El proceso server no tiene una sola instancia.");
      if (env.watch === true) fail("El proceso server tiene watch habilitado.");
      if (env.pm_cwd !== "/home/ubuntu/community-data-backend") {
        fail("El cwd de PM2 no coincide con el repositorio.");
      }
      if (env.pm_exec_path !== "/home/ubuntu/community-data-backend/server.js") {
        fail("El script de PM2 no es server.js.");
      }

      console.log(
        "[REMOTE] PM2 verificado: server online, pid " +
        processInfo.pid + ", Node " + (env.node_version || "desconocido")
      );
    });
  '
}

assert_pm2_configuration

export GIT_TERMINAL_PROMPT=0
old_sha="$(git rev-parse HEAD)"
git fetch --prune origin "$branch"
new_sha="$(git rev-parse "origin/$branch")"

git merge-base --is-ancestor "$old_sha" "$new_sha" ||
  fail "origin/main no es un avance fast-forward del commit instalado."

dependencies_changed=0
if ! git diff --quiet "$old_sha" "$new_sha" -- package.json package-lock.json; then
  dependencies_changed=1
fi

log "Commit instalado: ${old_sha:0:7}"
log "Commit disponible: ${new_sha:0:7}"

if test "$dependencies_changed" -eq 1; then
  log "package.json o package-lock.json cambiaron; se ejecutara npm ci."
else
  log "Las dependencias declaradas no cambiaron."
fi

if test "$dry_run" -eq 1; then
  log "DRY-RUN correcto. No se modifico Git, npm ni PM2."
  exit 0
fi

git merge --ff-only "$new_sha"

if test "$dependencies_changed" -eq 1 || ! test -d node_modules; then
  npm ci --omit=dev --no-audit --no-fund
elif ! npm ls --omit=dev --depth=0 >/dev/null 2>&1; then
  log "node_modules no coincide con el lockfile; se reparara con npm ci."
  npm ci --omit=dev --no-audit --no-fund
else
  log "node_modules es consistente; no hace falta reinstalar."
fi

node --check app.js
node --check server.js

while IFS= read -r -d '' javascript_file; do
  node --check "$javascript_file"
done < <(find api scripts -type f -name '*.js' -print0)

log "Sintaxis JavaScript valida con $(node --version)."

pm2 restart "$app_name" --update-env

ping_ready=0
for attempt in $(seq 1 30); do
  if curl --fail --silent --output /dev/null \
    --max-time 3 'http://127.0.0.1:3000/api/ping'; then
    ping_ready=1
    break
  fi
  sleep 1
done

test "$ping_ready" -eq 1 ||
  fail "El backend no respondio /api/ping en 30 segundos."

readiness_file="$(mktemp)"
trap 'rm -f "$readiness_file"' EXIT

database_status="$(
  curl --silent --show-error --output "$readiness_file" \
    --write-out '%{http_code}' --max-time 15 \
    'http://127.0.0.1:3000/api/mobile-app/versions'
)"

test "$database_status" = "200" ||
  fail "La comprobacion de base de datos devolvio HTTP $database_status."

node -e '
  JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
' "$readiness_file"

assert_pm2_configuration

if test -n "$(git status --porcelain=v1 --untracked-files=normal)"; then
  fail "El deploy dejo cambios inesperados en el repositorio remoto."
fi

final_sha="$(git rev-parse HEAD)"
log "DEPLOY_OK ${old_sha:0:7} -> ${final_sha:0:7}"
'@

    return $bash.Replace('__DRY_RUN__', $dryRunValue)
}

function Invoke-RemoteDeploy {
    param([string]$RepoRoot)

    $remoteBash = Get-RemoteBash -IsDryRun:$DryRun.IsPresent
    $utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
    $normalizedBash = $remoteBash -replace "`r`n", "`n"
    $payload = [Convert]::ToBase64String($utf8WithoutBom.GetBytes($normalizedBash))
    $remoteCommand = "printf '%s' '$payload' | base64 --decode | bash -se"

    $sshArguments = @(
        '-T',
        '-i', $KeyPath,
        '-o', 'BatchMode=yes',
        '-o', 'IdentitiesOnly=yes',
        '-o', 'StrictHostKeyChecking=yes',
        '-o', 'ConnectTimeout=10',
        '-o', 'ConnectionAttempts=1',
        '-o', 'ServerAliveInterval=15',
        '-o', 'ServerAliveCountMax=2',
        $SshTarget,
        $remoteCommand
    )

    Invoke-NativeText -FilePath $script:SshExe -Arguments $sshArguments `
        -WorkingDirectory $RepoRoot -Description 'Deploy remoto por SSH' `
        -ShowOutput | Out-Null
}

function Get-HttpResource {
    param([string]$Url)

    Add-Type -AssemblyName System.Net.Http

    $handler = New-Object System.Net.Http.HttpClientHandler
    $handler.AllowAutoRedirect = $false
    $handler.AutomaticDecompression = (
        [System.Net.DecompressionMethods]::GZip -bor
        [System.Net.DecompressionMethods]::Deflate
    )

    $client = New-Object System.Net.Http.HttpClient($handler)
    $client.Timeout = [TimeSpan]::FromSeconds(30)

    try {
        $response = $client.GetAsync($Url).GetAwaiter().GetResult()
        try {
            $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            $contentType = $null
            if ($response.Content.Headers.ContentType) {
                $contentType = $response.Content.Headers.ContentType.MediaType
            }

            return [pscustomobject]@{
                Url = $Url
                StatusCode = [int]$response.StatusCode
                ContentType = $contentType
                Body = $body
            }
        }
        finally {
            $response.Dispose()
        }
    }
    finally {
        $client.Dispose()
        $handler.Dispose()
    }
}

function Test-PublicEndpoints {
    Write-Section 'Validar API publica'

    $ping = Get-HttpResource -Url $PublicPingUrl
    if ($ping.StatusCode -ne 200) {
        throw "$PublicPingUrl devolvio HTTP $($ping.StatusCode)."
    }

    $database = Get-HttpResource -Url $PublicDatabaseUrl
    if ($database.StatusCode -ne 200) {
        throw "$PublicDatabaseUrl devolvio HTTP $($database.StatusCode)."
    }

    if ($database.ContentType -ne 'application/json') {
        throw "$PublicDatabaseUrl devolvio Content-Type '$($database.ContentType)'."
    }

    try {
        $database.Body | ConvertFrom-Json | Out-Null
    }
    catch {
        throw "$PublicDatabaseUrl no devolvio JSON valido."
    }

    Write-Success '/api/ping responde HTTP 200'
    Write-Success '/api/mobile-app/versions responde HTTP 200 con JSON valido'
}

$exitCode = 0
$mutex = $null
$mutexAcquired = $false
$repoRoot = $null
$initialBranch = $null
$initialHead = $null
$initialStatus = $null

try {
    $mutex = New-Object System.Threading.Mutex($false, 'Local\BienestarBackendDeploy')
    $mutexAcquired = $mutex.WaitOne(0)
    if (-not $mutexAcquired) {
        throw 'Ya hay otro deploy del backend en ejecucion desde esta PC.'
    }

    $repoRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
    $script:GitExe = Resolve-Executable -Name 'git.exe' -Candidates @(
        (Join-Path $env:ProgramFiles 'Git\cmd\git.exe'),
        (Join-Path $env:LOCALAPPDATA 'Programs\Git\cmd\git.exe')
    )
    $script:SshExe = Resolve-Executable -Name 'ssh.exe' -Candidates @(
        (Join-Path $env:WINDIR 'System32\OpenSSH\ssh.exe')
    )

    if (-not (Test-Path -LiteralPath $KeyPath -PathType Leaf)) {
        throw "No se encontro la clave SSH: $KeyPath"
    }

    $initialBranch = Invoke-GitText -Arguments @('branch', '--show-current') `
        -RepoRoot $repoRoot -Description 'Leer rama inicial'
    $initialHead = Invoke-GitText -Arguments @('rev-parse', 'HEAD') `
        -RepoRoot $repoRoot -Description 'Leer HEAD inicial'
    $initialStatus = Invoke-GitText -Arguments @('status', '--porcelain=v1') `
        -RepoRoot $repoRoot -Description 'Leer estado inicial'

    Write-Section 'Verificar repositorio y destino'
    Assert-LocalRepository -RepoRoot $repoRoot
    Write-Success "SSH: $SshTarget"
    Write-Success "Clave: $KeyPath"

    Confirm-ProductionDeploy

    Write-Section 'Ejecutar deploy remoto'
    Invoke-RemoteDeploy -RepoRoot $repoRoot

    if (-not $DryRun) {
        Test-PublicEndpoints
    }

    Write-Section 'Resumen'
    if ($DryRun) {
        Write-Success 'Dry-run completado; Git remoto y PM2 no fueron modificados.'
    }
    else {
        Write-Success 'Backend desplegado y verificado correctamente.'
    }
}
catch [System.OperationCanceledException] {
    Write-Warning $_.Exception.Message
    $exitCode = 0
}
catch {
    Write-Host ''
    Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
    $exitCode = 1
}
finally {
    if ($repoRoot -and $script:GitExe) {
        try {
            $endingBranch = Invoke-GitText -Arguments @('branch', '--show-current') `
                -RepoRoot $repoRoot -Description 'Leer rama final'
            $endingHead = Invoke-GitText -Arguments @('rev-parse', 'HEAD') `
                -RepoRoot $repoRoot -Description 'Leer HEAD final'
            $endingStatus = Invoke-GitText -Arguments @('status', '--porcelain=v1') `
                -RepoRoot $repoRoot -Description 'Leer estado final'

            if (
                $endingBranch -ne $initialBranch -or
                $endingHead -ne $initialHead -or
                $endingStatus -ne $initialStatus
            ) {
                Write-Warning 'El estado del repositorio local cambio durante el deploy.'
                $exitCode = 1
            }
            else {
                Write-Success 'El repositorio local no fue modificado.'
            }
        }
        catch {
            Write-Warning "No se pudo verificar el estado final local: $($_.Exception.Message)"
            $exitCode = 1
        }
    }

    if ($mutexAcquired -and $mutex) {
        $mutex.ReleaseMutex()
    }

    if ($mutex) {
        $mutex.Dispose()
    }
}

exit $exitCode
