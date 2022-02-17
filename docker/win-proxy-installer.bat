@echo off

powershell.exe -Command  $ErrorActionPreference = 'Stop'; (New-Object System.Net.WebClient).DownloadFile('https://s3-us-west-2.amazonaws.com/wavefront-cdn/windows/wavefront-proxy-setup.exe', 'C:\wavefront-proxy-setup.exe') ; Start-Process C:\wavefront-proxy-setup.exe -ArgumentList @('/server="%WAVEFRONT_URL%" /token="%WAVEFRONT_TOKEN%" /VERYSILENT /SUPPRESSMSGBOXES') -Wait ; Remove-Item C:\wavefront-proxy-setup.exe -Force ; Start-Sleep -s 10 ; $ProgFilePath=${Env:ProgramFiles(x86)} ; Get-Content -Path ${ProgFilePath}\Wavefront\logs\wavefront.log -Wait
