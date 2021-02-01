param (
    [string]$sourcedir = (Resolve-Path "$PSScriptRoot\.."),
    [string]$imagename = "fdb",
    # By default we want to leave one CPU core for the OS so the user has some minimal control over the system
    [string]$cpus = (Get-WmiObject -Class Win32_Processor).NumberOfLogicalProcessors - 1,
    # We want to leave at least 1GB of memory for the OS
    [string]$memory = (Get-WmiObject Win32_ComputerSystem).TotalPhysicalMemory - [Math]::Pow(2, 30),
    [Parameter(Mandatory=$true)][string]$builddir,
    [switch]$dryrun = $false,
    [switch]$forceConfigure = $false,
    [Parameter(Position=0)][string]$target = "installer"
)

$exponent = 0
$memory = $memory.ToUpper()
if ($memory.EndsWith("K")) {
    $exponent = 10
} elseif ($memory.EndsWith("M")) {
    $exponent = 20
} elseif ($memory.EndsWith("G")) {
    $exponent = 30
} elseif ($memory.EndsWith("T")) {
    $exponent = 40
}
if ($exponent -gt 0) {
    $memory = $memory.Substring(0, $memory.Length - 1) * [Math]::Pow(2, $exponent)
}

$buildCommand = [string]::Format("Get-Content {0}\build\Dockerfile.windows | docker build -t {1} -m {2} -", 
                                 $sourcedir, $imagename, [Math]::Min(16 * [Math]::Pow(2, 30), $memory))
if($dryrun) {
    Write-Output $buildCommand
} else {
    Invoke-Expression -Command $buildCommand
}

# Write build instructions into file
$batFile = "$builddir\docker_command.cmd"
"C:\BuildTools\Common7\Tools\VsDevCmd.bat" | Out-File $batFile 
"cd \fdbbuild" | Out-File -Append $batFile
if ($forceConfigure -or ![System.IO.File]::Exists("$builddir\CMakeCache.txt") -or ($target -eq "configure")) {
    "cmake -G ""Visual Studio 16 2019"" -A x64 -T""ClangCL"" -DCMAKE_BUILD_TYPE=Release C:\foundationdb" | Out-File -Append $batFile
}
if ($target -ne "configure") {
    "msbuild /p:CL_MPCount=$cpus /m /p:UseMultiToolTask=true /p:Configuration=Release foundationdb.sln" | Out-File -Append $batFile
}

$runCommand = [string]::Format("docker run -v {0}:C:\foundationdb -v {1}:C:\fdbbuild --name fdb-build -m {2} --cpus={3} --rm {4} {5}",
                                $sourcedir, $builddir, $memory, $cpus, $imagename, $batFile);
if ($dryrun) {
    Write-Output $runCommand
} else {
    Invoke-Expression $runCommand
}