import pandas as pd
import sys,os
os.getcwd()
dlist = os.listdir()
benedict = {}
target_files = [file for file in dlist if file.startswith('TenantDetractors')]
for file in target_files:
    benedict[file] = pd.read_csv(file)
All_Detractors = pd.concat(benedict.values(), ignore_index=True)
All_Detractors = All_Detractors.iloc[:,1:] #Remove extraneous index
All_Detractors.to_csv('TenantDetractorsFull.csv', index=False)

Col_s=['TenantId','Name','Country','CountryCode','Region','City','CommunicationLanguage','CommunicationCulture',
     'CreatedDate','CreateDateOfFirstSubscription','PurchaseDateOfFirstNonTrialSubscription','TenantState','TenantType',
     'EXOEnabledUsers','LicensedUsers','LYOEnabledUsers','SPOEnabledUsers','OD4BEnabledUsers','ProPlusEnabledUsers','TotalUsers',
     'EDUSubscriptionsCount','ExchangeSubscriptionsCount','LyncSubscriptionsCount','PaidSubscriptionCount',
     'ProjectSubscriptionsCount','SharePointSubscriptionsCount','TotalSubscriptionCount','TrialSubscriptionCount',
     'VisioSubscriptionsCount','HasEducation','HasCharity','HasGovernment','HasExchange','HasLync','HasSharePoint','HasProPlus',
     'HasYammer','HasSubscription','HasProject','HasPaid','HasVisio','HasTrial','DomainCount','CommerceTenantTagsArray',
     'CommerceTenantTagsCount','AssignedPlanServiceTypeArray','AssignedPlanCount','PartnerTenantCount','SubscriptionCount',
     'ConciergeInfoIsConceirge','IsConcierge','ConciergeInfoIsManualAdmittance','ConciergeInfoProgramId',
     'CompanyLastDirSyncTime','DirectoryExtensionsSyncEnabled','DirSyncEnabled','PasswordSyncEnabled','PasswordSyncTime',
     'PasswordWriteBackEnabled','IsDonMT','IsViral','IsTest','IsQuickStart','IsFastTrackTenant','HasSKUE3','HasSKUE5',
     'SPOEnvironment','IsRestrictRmsViralSignUp','IsMSODSDeleted','TotalGroupCount','CurrentDefaultDomain',
     'O365TenantReleaseTrack','DefaultDataLocation','SnapshotTime','MSODSTenant_CompanyTags','MSODSTenant_ExtensionAttributes',
     'MSODSTenant_DirSyncEnabled','CPTenant_ChannelName','FirstTicketAge','FirstTicketDate','LastTicketDate',
     'TotalTickets','MinCloseTime','MaxCloseTime','MedCloseTime','SubscriptionEndDate','DaysToSubEndDate','SubscriptionStartDate',
     'DaysTicketOpenAfterSubStart','TicketsFeedRecency','NumberOfTickets_Past7Days','NumberOfTickets_Past28Days',
     'NumberOfTickets_Past90Days','NumberOfTickets_Past180Days','NumberOfTickets_Past360Days','Age_at_First_Ticket_Bucket',
     'PercentOfTickets','TotalTickets1Year','Percent_Of_Tickets_Bucket','Exchange_unable_to_connect_sync_with_exchange',
     'SharePoint_manage_sites_documents_and_lists','Admin_Get_reports_insights_and_usage_patterns_for_my_Office_365_tenant',
     'Office_Client_Use_Office_apps_including_Mac','Admin_Find_and_signup_for_the_correct_Office_365_plan',
     'Dynamics_CRM_Setup_and_use_Dynamics_CRM_and_Parature_services','Intune_Download_Setup_and_Use_Intune',
     'OneDrive_Setup_OneDrive_and_sync_my_documents','Admin_Sign_in_and_password_issues',
     'Mobile_Connect_and_configure_mobile_devices','Setup_and_use_Stream','Exchange_Use_calendar_free_busy_and_contacts',
     'Install_setup_and_use_Power_Bi','Commerce_Manage_bills_payments_subscriptions_and_licenses',
     'Exchange_Migrate_my_data_to_Office_365','Office_Client_Download_install_and_activate_Office_apps_including_Mac',
     'Other','Setup_and_use_OneNote','Exchange_Setup_and_manage_mailbox_exchange_online','Setup_and_use_PowerPoint',
     'Exchange_Enable_hybrid_capabilities','Setup_compliance_features_like_Archive_Retention_Litigation_eDiscovery_and_MDM',
     'Admin_Setup_domain_and_DNS_settings_for_Office_365','Exchange_Use_OWA_Outlook_Web_App',
     'Outlook_Setup_and_use_Outlook_including_Mac','Office_Client_Word','Yammer_Setup_and_use_Yammer_services',
     'Office_Client_Excel','Prevent_user_accounts_from_getting_compromised','Send_and_receive_mail_on_time',
     'Admin_Global_Office_365_setup_and_administration_DirSync_ADFS_Global_Exchange_settings',
     'Admin_Manage_my_users_groups_and_resources','Setup_and_use_Project','Skype_Setup_and_use_Skype_services',
     'Keep_mailboxes_free_of_spam_and_viruses','Project_and_Planner_manage_projects_and_plans','Setup_and_use_Delve_Analytics',
     'Teams_Download_Setup_and_Use_Microsoft_Teams',	'L7TotalUsage',	'L14TotalUsage',	'L28TotalUsage',	'ProjectL7Usage',	
     'PublisherL7Usage',	'OutlookL7Usage',	'WordL7Usage',	'VisioL7Usage',	'OneNoteL7Usage',	'ExcelL7Usage',	'AccessL7Usage',
     	'LyncL7Usage',	'PowerPointL7Usage',	'ProjectL14Usage',	'PublisherL14Usage',	'OutlookL14Usage',	'WordL14Usage',	'VisioL14Usage',
      'OneNoteL14Usage',	'ExcelL14Usage',	'AccessL14Usage',	'LyncL14Usage',	'PowerPointL14Usage',	'ProjectL28Usage',	'PublisherL28Usage',	
      'OutlookL28Usage',	'WordL28Usage',	'VisioL28Usage',	'OneNoteL28Usage',	'ExcelL28Usage',	'AccessL28Usage',	'LyncL28Usage',	
      'PowerPointL28Usage',	'ProjectL7Usage_Percent',	'PublisherL7Usage_Percent',	'OutlookL7Usage_Percent',	'WordL7Usage_Percent',	
    'VisioL7Usage_Percent',	'OneNoteL7Usage_Percent',	'ExcelL7Usage_Percent',	'AccessL7Usage_Percent',	'LyncL7Usage_Percent',	
    'PowerPointL7Usage_Percent',	'ProjectL14Usage_Percent',	'PublisherL14Usage_Percent',	'OutlookL14Usage_Percent',	'WordL14Usage_Percent',	
    'VisioL14Usage_Percent',	'OneNoteL14Usage_Percent',	'ExcelL14Usage_Percent',	'AccessL14Usage_Percent',	'LyncL14Usage_Percent',	
    'PowerPointL14Usage_Percent',	'ProjectL28Usage_Percent',	'PublisherL28Usage_Percent',	'OutlookL28Usage_Percent',	'WordL28Usage_Percent',
    	'VisioL28Usage_Percent',	'OneNoteL28Usage_Percent',	'ExcelL28Usage_Percent',	'AccessL28Usage_Percent',	'LyncL28Usage_Percent',	
      'PowerPointL28Usage_Percent',	'ProjectL7Usage_Percent_Desc',	'PublisherL7Usage_Percent_Desc',	'OutlookL7Usage_Percent_Desc',	
    'WordL7Usage_Percent_Desc',	'VisioL7Usage_Percent_Desc',	'OneNoteL7Usage_Percent_Desc',	'ExcelL7Usage_Percent_Desc',	
    'AccessL7Usage_Percent_Desc',	'LyncL7Usage_Percent_Desc',	'PowerPointL7Usage_Percent_Desc',	'ProjectL14Usage_Percent_Desc',	
    'PublisherL14Usage_Percent_Desc',	'OutlookL14Usage_Percent_Desc',	'WordL14Usage_Percent_Desc',	'VisioL14Usage_Percent_Desc',	
    'OneNoteL14Usage_Percent_Desc',	'ExcelL14Usage_Percent_Desc',	'AccessL14Usage_Percent_Desc',	'LyncL14Usage_Percent_Desc',	
    'PowerPointL14Usage_Percent_Desc',	'ProjectL28Usage_Percent_Desc',	'PublisherL28Usage_Percent_Desc',	'OutlookL28Usage_Percent_Desc',	
    'WordL28Usage_Percent_Desc',	'VisioL28Usage_Percent_Desc',	'OneNoteL28Usage_Percent_Desc',	'ExcelL28Usage_Percent_Desc',	
    'AccessL28Usage_Percent_Desc',	'LyncL28Usage_Percent_Desc',	'PowerPointL28Usage_Percent_Desc',	'Win10L7Usage',	'WinOtherL7Usage',	
    'MacL7Usage',	'iOSL7Usage',	'IPhoneL7Usage',	'AndroidL7Usage',	'Win10L14Usage',	'WinOtherL14Usage',	'MacL14Usage',	'iOSL14Usage',	
    'IPhoneL14Usage',	'AndroidL14Usage',	'Win10L28Usage',	'WinOtherL28Usage',	'MacL28Usage',	'iOSL28Usage',	'IPhoneL28Usage',	'AndroidL28Usage',
    'Win10L7Usage_Percent',	'WinOtherL7Usage_Percent',	'MacL7Usage_Percent',	'iOSL7Usage_Percent',	'IPhoneL7Usage_Percent',	'AndroidL7Usage_Percent',
    'Win10L14Usage_Percent',	'WinOtherL14Usage_Percent',	'MacL14Usage_Percent',	'iOSL14Usage_Percent',	'IPhoneL14Usage_Percent',	
    'AndroidL14Usage_Percent',	'Win10L28Usage_Percent',	'WinOtherL28Usage_Percent',	'MacL28Usage_Percent',	'iOSL28Usage_Percent',	
    'IPhoneL28Usage_Percent',	'AndroidL28Usage_Percent',	'Win10L7Usage_Percent_Desc',	'WinOtherL7Usage_Percent_Desc',	'MacL7Usage_Percent_Desc',	
    'iOSL7Usage_Percent_Desc',	'IPhoneL7Usage_Percent_Desc',	'AndroidL7Usage_Percent_Desc',	'Win10L14Usage_Percent_Desc',	
    'WinOtherL14Usage_Percent_Desc',	'MacL14Usage_Percent_Desc',	'iOSL14Usage_Percent_Desc',	'IPhoneL14Usage_Percent_Desc',	
    'AndroidL14Usage_Percent_Desc',	'Win10L28Usage_Percent_Desc',	'WinOtherL28Usage_Percent_Desc',	'MacL28Usage_Percent_Desc',	
    'iOSL28Usage_Percent_Desc',	'IPhoneL28Usage_Percent_Desc',	'AndroidL28Usage_Percent_Desc',	'LatestFeedbackDateTime',	'TotalFeedbacks',	
    'LatestSATScore',	'LatestSATType',	'CompositeSATScore',	'CompositeSATType', 'LastCol'
]

def detractors_from_split(ids,split_num):
        data = pd.read_csv("/data/DetractorPrediction/NPSPredictiveModelAttributesTenant_Split_"+str(split_num)+".csv",
        sep=',', header=None, names=Col_s, index_col=False)
        data = data[data.TenantId.isin(All_Detractors.TenantId)]
        data.to_csv('FullDetractorData_Split'+str(split_num)+'.csv')
        
