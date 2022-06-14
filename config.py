import os

SP_cfg = {
    "SharePoint_BaseSite": r"imecinternational.sharepoint.com",
    "SharePoint_TargetSite": r"/sites/Team-ToolData",
    "SharePoint_DriveName": r"Documents",
    "SharePoint_Path": {
        "eDR": r"/KLA_eDR/",
        "CDSEM": r"/Hitachi_CD-SEMs/",
        "KLA2925": r"/KLA2925/"
    },
    "SharePoint_AccessAccount": "appm_con@imecinternational.onmicrosoft.com",
    
    "AD_App_client_id": r"415650c1-1005-494c-b25d-08dc96807886",
    "AD_App_authority": r"https://login.microsoftonline.com/a72d5a72-25ee-40f0-9bd1-067cb5b770d4",
}
fname_prefix = "OS"


logfolderlocation = "//LAPTOP-UONIE73C/Temp/Logs/"
isFolder = 'Y'
LocalCSVLocation = "C:/Users/Francis John Picaso/Repositories/Teo/Output/"
UploadToSharePoint = "N"

tool = "KLA2925"