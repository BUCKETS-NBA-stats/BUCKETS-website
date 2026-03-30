# scripts/local/create_scheduled_task.ps1
#
# Run ONCE from an elevated (admin) PowerShell to register the BUCKETS daily refresh task.
# Usage:
#   Set-Location C:\BUCKETS\prod
#   .\scripts\local\create_scheduled_task.ps1
#
# Requirements:
#   - Must be run as Administrator
#   - Machine timezone should be set to Central Time for the 4:00 AM trigger to be correct
#
# Log output: C:\BUCKETS\prod\logs\update.log  (written by update.ps1 itself)
#
# CTG login: set CTG_EMAIL and CTG_PASSWORD as system environment variables before
# the first scheduled run (System Properties > Environment Variables > System variables).

$ErrorActionPreference = "Stop"

$TaskName   = "BUCKETS Daily Refresh (2025-26)"
$ScriptPath = "C:\BUCKETS\prod\scripts\local\update.ps1"
$WorkingDir = "C:\BUCKETS\prod"
$RunAt      = "04:00"

# ---- Check for admin rights ----
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)
if (-not $isAdmin) {
    throw "This script must be run from an elevated (Administrator) PowerShell."
}

# ---- Identify the user account the task will run as ----
$RunAsUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
Write-Host "Task will run as: $RunAsUser"
Write-Host "Enter your Windows password so the task can run when you are logged out."
Write-Host ""
$SecurePassword = Read-Host -Prompt "Windows password for $RunAsUser" -AsSecureString
$PlainPassword  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
)

try {
    # ---- Action ----
    $Action = New-ScheduledTaskAction `
        -Execute    "powershell.exe" `
        -Argument   "-ExecutionPolicy Bypass -NonInteractive -WindowStyle Hidden -File `"$ScriptPath`"" `
        -WorkingDirectory $WorkingDir

    # ---- Trigger: daily at 4:00 AM (machine local time) ----
    $Trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

    # ---- Settings ----
    $Settings = New-ScheduledTaskSettingsSet `
        -MultipleInstances    IgnoreNew `
        -ExecutionTimeLimit   (New-TimeSpan -Hours 2) `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries

    # ---- Register ----
    Register-ScheduledTask `
        -TaskName  $TaskName `
        -Action    $Action `
        -Trigger   $Trigger `
        -Settings  $Settings `
        -RunLevel  Highest `
        -User      $RunAsUser `
        -Password  $PlainPassword `
        -Force | Out-Null

    Write-Host ""
    Write-Host "Task registered successfully."
    Write-Host "  Name      : $TaskName"
    Write-Host "  Runs at   : $RunAt daily (machine local time)"
    Write-Host "  Script    : $ScriptPath"
    Write-Host "  Work dir  : $WorkingDir"
    Write-Host "  Log file  : $WorkingDir\logs\update.log"
    Write-Host ""
    Write-Host "Useful commands:"
    Write-Host "  Verify : Get-ScheduledTask -TaskName '$TaskName' | Get-ScheduledTaskInfo"
    Write-Host "  Run now: Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host "  Remove : Unregister-ScheduledTask -TaskName '$TaskName' -Confirm:`$false"
}
finally {
    # Clear plaintext password from memory
    $PlainPassword = $null
    [System.GC]::Collect()
}
