# This code shall be used only with approval by the author (NitMatGeo)

Function CallAPI {
    Param(
        [parameter(Mandatory = $true)][string]$apiURL,
        [parameter(Mandatory = $true)][string]$apiMethod,
        [parameter(Mandatory = $true)]$apiToken,
        [parameter(Mandatory = $false)][string]$apiBody,
        [parameter(Mandatory = $false)][string]$apiContentType
    )

    $apiHeaders = @{
        'Content-Type'  = 'application/json'
        'Authorization' = "Bearer $apiToken"
    }

    try {
        if ($apiBody) {
            $response = Invoke-RestMethod -Uri $apiURL -Headers $apiHeaders -Method $apiMethod -apiContentType $apiContentType -Body $apiBody
        }
        else {
            $response = Invoke-RestMethod -Uri $apiURL -Headers $apiHeaders -Method $apiMethod
        }
    }

    catch [System.Net.WebException] {
        $ex = $_.Exception
        try {
            if ($null -ne $ex.Response) {
                $streamReader = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream())
                $readException = $streamReader.ReadToEnd() | ConvertFrom-Json
                Write-Host "Error Code is: $($readException.error.code)"
            }
            else {
                $message = $readException.error.message
                if ($message) {
                    Write-Error $message
                }
                else {
                    Write-Error -Exception $ex
                }               
            }
        }
        catch {
            throw;
        } 		
    }

    return $response
}

## To authenticate with SPN
$ClientID = ""
$ClientSecret = ""
$TenantID = ""
$SubscriptionID = ""

Write-Host "Establishing Azure Connection..."
$credentials = New-Object System.Management.Automation.PSCredential ($ClientID, (convertto-securestring $ClientSecret -asplaintext -force))
$authSPN = Connect-AzAccount -ServicePrincipal -Credential $credentials -Tenant $TenantID -SubscriptionId $SubscriptionID
Write-Host "Azure Connection established."

$apiToken = $authSPN.AccessToken
$apiResponse = CallAPI -apiURL $apiURL -apiMethod "Post" -apiContentType "application/json" -apiToken $apiToken

