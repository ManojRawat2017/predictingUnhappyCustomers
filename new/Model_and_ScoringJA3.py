import numpy as np
import pandas as pd
import math
from sklearn.metrics import classification_report
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from sklearn import preprocessing
seed = 7
np.random.seed(seed)

### MODEL PREP ###

#since the Pyscope is not working, data in COSMOS was dumped as text file, which is imported as below. Labeled data was extracted from SQL and those tenantIDs were appended with metadata in COSMOS and expoted as tect file
Col=['TenantId2','Det','TenantId','Name','Country','CountryCode','Region','City','CommunicationLanguage','CommunicationCulture',
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
     'O365TenantReleaseTrack','DefaultDataLocation',
     'SnapshotTime','MSODSTenant_CompanyTags','MSODSTenant_ExtensionAttributes',
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
     'Teams_Download_Setup_and_Use_Microsoft_Teams',	'L7TotalUsage',	'L14TotalUsage',	'L28TotalUsage',	'ProjectL7Usage',	'PublisherL7Usage',	'OutlookL7Usage',	'WordL7Usage',	'VisioL7Usage',	'OneNoteL7Usage',	'ExcelL7Usage',	'AccessL7Usage',	'LyncL7Usage',	'PowerPointL7Usage',	'ProjectL14Usage',	'PublisherL14Usage',	'OutlookL14Usage',	'WordL14Usage',	'VisioL14Usage',	'OneNoteL14Usage',	'ExcelL14Usage',	'AccessL14Usage',	'LyncL14Usage',	'PowerPointL14Usage',	'ProjectL28Usage',	'PublisherL28Usage',	'OutlookL28Usage',	'WordL28Usage',	'VisioL28Usage',	'OneNoteL28Usage',	'ExcelL28Usage',	'AccessL28Usage',	'LyncL28Usage',	'PowerPointL28Usage',	'ProjectL7Usage_Percent',	'PublisherL7Usage_Percent',	'OutlookL7Usage_Percent',	'WordL7Usage_Percent',	'VisioL7Usage_Percent',	'OneNoteL7Usage_Percent',	'ExcelL7Usage_Percent',	'AccessL7Usage_Percent',	'LyncL7Usage_Percent',	'PowerPointL7Usage_Percent',	'ProjectL14Usage_Percent',	'PublisherL14Usage_Percent',	'OutlookL14Usage_Percent',	'WordL14Usage_Percent',	'VisioL14Usage_Percent',	'OneNoteL14Usage_Percent',	'ExcelL14Usage_Percent',	'AccessL14Usage_Percent',	'LyncL14Usage_Percent',	'PowerPointL14Usage_Percent',	'ProjectL28Usage_Percent',	'PublisherL28Usage_Percent',	'OutlookL28Usage_Percent',	'WordL28Usage_Percent',	'VisioL28Usage_Percent',	'OneNoteL28Usage_Percent',	'ExcelL28Usage_Percent',	'AccessL28Usage_Percent',	'LyncL28Usage_Percent',	'PowerPointL28Usage_Percent',	'ProjectL7Usage_Percent_Desc',	'PublisherL7Usage_Percent_Desc',	'OutlookL7Usage_Percent_Desc',	'WordL7Usage_Percent_Desc',	'VisioL7Usage_Percent_Desc',	'OneNoteL7Usage_Percent_Desc',	'ExcelL7Usage_Percent_Desc',	'AccessL7Usage_Percent_Desc',	'LyncL7Usage_Percent_Desc',	'PowerPointL7Usage_Percent_Desc',	'ProjectL14Usage_Percent_Desc',	'PublisherL14Usage_Percent_Desc',	'OutlookL14Usage_Percent_Desc',	'WordL14Usage_Percent_Desc',	'VisioL14Usage_Percent_Desc',	'OneNoteL14Usage_Percent_Desc',	'ExcelL14Usage_Percent_Desc',	'AccessL14Usage_Percent_Desc',	'LyncL14Usage_Percent_Desc',	'PowerPointL14Usage_Percent_Desc',	'ProjectL28Usage_Percent_Desc',	'PublisherL28Usage_Percent_Desc',	'OutlookL28Usage_Percent_Desc',	'WordL28Usage_Percent_Desc',	'VisioL28Usage_Percent_Desc',	'OneNoteL28Usage_Percent_Desc',	'ExcelL28Usage_Percent_Desc',	'AccessL28Usage_Percent_Desc',	'LyncL28Usage_Percent_Desc',	'PowerPointL28Usage_Percent_Desc',	'Win10L7Usage',	'WinOtherL7Usage',	'MacL7Usage',	'iOSL7Usage',	'IPhoneL7Usage',	'AndroidL7Usage',	'Win10L14Usage',	'WinOtherL14Usage',	'MacL14Usage',	'iOSL14Usage',	'IPhoneL14Usage',	'AndroidL14Usage',	'Win10L28Usage',	'WinOtherL28Usage',	'MacL28Usage',	'iOSL28Usage',	'IPhoneL28Usage',	'AndroidL28Usage',	'Win10L7Usage_Percent',	'WinOtherL7Usage_Percent',	'MacL7Usage_Percent',	'iOSL7Usage_Percent',	'IPhoneL7Usage_Percent',	'AndroidL7Usage_Percent',	'Win10L14Usage_Percent',	'WinOtherL14Usage_Percent',	'MacL14Usage_Percent',	'iOSL14Usage_Percent',	'IPhoneL14Usage_Percent',	'AndroidL14Usage_Percent',	'Win10L28Usage_Percent',	'WinOtherL28Usage_Percent',	'MacL28Usage_Percent',	'iOSL28Usage_Percent',	'IPhoneL28Usage_Percent',	'AndroidL28Usage_Percent',	'Win10L7Usage_Percent_Desc',	'WinOtherL7Usage_Percent_Desc',	'MacL7Usage_Percent_Desc',	'iOSL7Usage_Percent_Desc',	'IPhoneL7Usage_Percent_Desc',	'AndroidL7Usage_Percent_Desc',	'Win10L14Usage_Percent_Desc',	'WinOtherL14Usage_Percent_Desc',	'MacL14Usage_Percent_Desc',	'iOSL14Usage_Percent_Desc',	'IPhoneL14Usage_Percent_Desc',	'AndroidL14Usage_Percent_Desc',	'Win10L28Usage_Percent_Desc',	'WinOtherL28Usage_Percent_Desc',	'MacL28Usage_Percent_Desc',	'iOSL28Usage_Percent_Desc',	'IPhoneL28Usage_Percent_Desc',	'AndroidL28Usage_Percent_Desc',	'LatestFeedbackDateTime',	'TotalFeedbacks',	'LatestSATScore',	'LatestSATType',	'CompositeSATScore',	'CompositeSATType',
]

#data = pd.read_csv("/home/ProSatDeepNets/DetractorPred/data/AppendLabel.txt" ,sep='\t',  lineterminator='\n', names=Col)
#data = pd.read_csv("C:/Users/mrawat/Documents/OneDrive - Microsoft/O365/MLmodel/AppendLabel.txt" ,sep='\t',  lineterminator='\n', names=Col, index_col=False)
data = pd.read_csv("AppendLabel.txt" ,sep='\t',  lineterminator='\n', names=Col)

data=data.drop(['CommunicationCulture',	'CreatedDate',	'CreateDateOfFirstSubscription',	'PurchaseDateOfFirstNonTrialSubscription',	
'CommerceTenantTagsArray','AssignedPlanServiceTypeArray','ConciergeInfoIsConceirge',	'ConciergeInfoProgramId',	
'CompanyLastDirSyncTime',	'PasswordSyncTime','CurrentDefaultDomain','SnapshotTime',	'MSODSTenant_CompanyTags',	
'MSODSTenant_ExtensionAttributes',	'MSODSTenant_DirSyncEnabled',	'FirstTicketDate',	'LastTicketDate',	'SubscriptionEndDate',	'SubscriptionStartDate',	'Percent_Of_Tickets_Bucket',	
'LatestFeedbackDateTime',	'TenantId2',	'TenantId',	'Name',	'Country',	'City', 'DefaultDataLocation'],axis=1)

data['CompositeSATType']=data['CompositeSATType'].str.split('\r', expand=True)[0]
data.loc[data['LatestSATType'].isnull(), 'LatestSATType'] ='No_SAT'
data.loc[data['CompositeSATType']== '', 'CompositeSATType'] ='No_SAT'

#In Numeric columns set empty to zero and convert the variable to Numeric
Emptyto_Zero=[ 'EXOEnabledUsers',	'LicensedUsers',	'LYOEnabledUsers',	'SPOEnabledUsers',	'OD4BEnabledUsers',	'ProPlusEnabledUsers',	'TotalUsers',	'EDUSubscriptionsCount',	
'ExchangeSubscriptionsCount',	'LyncSubscriptionsCount',	'PaidSubscriptionCount',	'ProjectSubscriptionsCount',	'SharePointSubscriptionsCount',	'TotalSubscriptionCount',	
'TrialSubscriptionCount',	'VisioSubscriptionsCount',	'SubscriptionCount',	'FirstTicketAge',	'TotalTickets',	'MinCloseTime',	'MaxCloseTime',	'MedCloseTime',	'DaysToSubEndDate',
'DaysTicketOpenAfterSubStart',	'TicketsFeedRecency',	'NumberOfTickets_Past7Days',	'NumberOfTickets_Past28Days',	'NumberOfTickets_Past90Days',	'NumberOfTickets_Past180Days',
'NumberOfTickets_Past360Days',	'PercentOfTickets',	'TotalTickets1Year',	'Exchange_unable_to_connect_sync_with_exchange',	'SharePoint_manage_sites_documents_and_lists',
'Admin_Get_reports_insights_and_usage_patterns_for_my_Office_365_tenant',	'Office_Client_Use_Office_apps_including_Mac',	'Admin_Find_and_signup_for_the_correct_Office_365_plan',	
'Dynamics_CRM_Setup_and_use_Dynamics_CRM_and_Parature_services',	'Intune_Download_Setup_and_Use_Intune',	'OneDrive_Setup_OneDrive_and_sync_my_documents',	'Admin_Sign_in_and_password_issues',
'Mobile_Connect_and_configure_mobile_devices',	'Setup_and_use_Stream',	'Exchange_Use_calendar_free_busy_and_contacts',	'Install_setup_and_use_Power_Bi',	
'Commerce_Manage_bills_payments_subscriptions_and_licenses',	'Exchange_Migrate_my_data_to_Office_365',	'Office_Client_Download_install_and_activate_Office_apps_including_Mac',	'Other',	
'Setup_and_use_OneNote',	'Exchange_Setup_and_manage_mailbox_exchange_online',	'Setup_and_use_PowerPoint',	'Exchange_Enable_hybrid_capabilities',	
'Setup_compliance_features_like_Archive_Retention_Litigation_eDiscovery_and_MDM',	'Admin_Setup_domain_and_DNS_settings_for_Office_365',	'Exchange_Use_OWA_Outlook_Web_App',	
'Outlook_Setup_and_use_Outlook_including_Mac',	'Office_Client_Word',	'Yammer_Setup_and_use_Yammer_services',	'Office_Client_Excel',	'Prevent_user_accounts_from_getting_compromised',	
'Send_and_receive_mail_on_time',	'Admin_Global_Office_365_setup_and_administration_DirSync_ADFS_Global_Exchange_settings',	'Admin_Manage_my_users_groups_and_resources',	'Setup_and_use_Project',	
'Skype_Setup_and_use_Skype_services',	'Keep_mailboxes_free_of_spam_and_viruses',	'Project_and_Planner_manage_projects_and_plans',	'Setup_and_use_Delve_Analytics',	
'Teams_Download_Setup_and_Use_Microsoft_Teams',	'L7TotalUsage',	'L14TotalUsage',	'L28TotalUsage',	'ProjectL7Usage',	'PublisherL7Usage',	'OutlookL7Usage',	'WordL7Usage',	
'VisioL7Usage',	'OneNoteL7Usage',	'ExcelL7Usage',	'AccessL7Usage',	'LyncL7Usage',	'PowerPointL7Usage',	'ProjectL14Usage',	'PublisherL14Usage',	'OutlookL14Usage',	'WordL14Usage',	
'VisioL14Usage',	'OneNoteL14Usage',	'ExcelL14Usage',	'AccessL14Usage',	'LyncL14Usage',	'PowerPointL14Usage',	'ProjectL28Usage',	'PublisherL28Usage',	'OutlookL28Usage',	'WordL28Usage',	
'VisioL28Usage',	'OneNoteL28Usage',	'ExcelL28Usage',	'AccessL28Usage',	'LyncL28Usage',	'PowerPointL28Usage',	'ProjectL7Usage_Percent',	'PublisherL7Usage_Percent',	'OutlookL7Usage_Percent',	
'WordL7Usage_Percent',	'VisioL7Usage_Percent',	'OneNoteL7Usage_Percent',	'ExcelL7Usage_Percent',	'AccessL7Usage_Percent',	'LyncL7Usage_Percent',	'PowerPointL7Usage_Percent',	'ProjectL14Usage_Percent',	
'PublisherL14Usage_Percent',	'OutlookL14Usage_Percent',	'WordL14Usage_Percent',	'VisioL14Usage_Percent',	'OneNoteL14Usage_Percent',	'ExcelL14Usage_Percent',	'AccessL14Usage_Percent',	
'LyncL14Usage_Percent',	'PowerPointL14Usage_Percent',	'ProjectL28Usage_Percent',	'PublisherL28Usage_Percent',	'OutlookL28Usage_Percent',	'WordL28Usage_Percent',	'VisioL28Usage_Percent',	
'OneNoteL28Usage_Percent',	'ExcelL28Usage_Percent',	'AccessL28Usage_Percent',	'LyncL28Usage_Percent',	'PowerPointL28Usage_Percent',	'Win10L7Usage',	'WinOtherL7Usage',	'MacL7Usage',	'iOSL7Usage',	
'IPhoneL7Usage',	'AndroidL7Usage',	'Win10L14Usage',	'WinOtherL14Usage',	'MacL14Usage',	'iOSL14Usage',	'IPhoneL14Usage',	'AndroidL14Usage',	'Win10L28Usage',	'WinOtherL28Usage',	'MacL28Usage',	
'iOSL28Usage',	'IPhoneL28Usage',	'AndroidL28Usage',	'Win10L7Usage_Percent',	'WinOtherL7Usage_Percent',	'MacL7Usage_Percent',	'iOSL7Usage_Percent',	'IPhoneL7Usage_Percent',	'AndroidL7Usage_Percent',	
'Win10L14Usage_Percent',	'WinOtherL14Usage_Percent',	'MacL14Usage_Percent',	'iOSL14Usage_Percent',	'IPhoneL14Usage_Percent',	'AndroidL14Usage_Percent',	'Win10L28Usage_Percent',	'WinOtherL28Usage_Percent',	
'MacL28Usage_Percent',	'iOSL28Usage_Percent',	'IPhoneL28Usage_Percent',	'AndroidL28Usage_Percent',	'TotalFeedbacks',	'LatestSATScore',	'CompositeSATScore','CommerceTenantTagsCount', 
'AssignedPlanCount','PartnerTenantCount','TotalGroupCount','DomainCount'
]

for x in Emptyto_Zero:
    data.loc[data[x].isnull(), x] =0
 #In Numeric columns set empty to zero and convert the variable to Numeric

 #make a list of Categorial variables, set empty value to No_Data and convert string to Cat var
StrTo_Cat=['TenantState',	'TenantType',	'HasEducation',	'HasCharity',	'HasGovernment',	'HasExchange',	'HasLync',	'HasSharePoint',	'HasProPlus',	'HasYammer',	'HasSubscription',	'HasProject',	
'HasPaid',	'HasVisio',	'HasTrial',	'IsConcierge',	'DirectoryExtensionsSyncEnabled',	'DirSyncEnabled',	'PasswordSyncEnabled',	'PasswordWriteBackEnabled',	'IsDonMT',	'IsViral',	'IsTest',	'IsQuickStart',	
'IsFastTrackTenant',	'HasSKUE3',	'HasSKUE5',	'SPOEnvironment',	'IsRestrictRmsViralSignUp',	'IsMSODSDeleted',	'Det',	'CommunicationLanguage',	'ConciergeInfoIsManualAdmittance',	'CPTenant_ChannelName',	
'Age_at_First_Ticket_Bucket',	'ProjectL7Usage_Percent_Desc',	'PublisherL7Usage_Percent_Desc',	'OutlookL7Usage_Percent_Desc',	'WordL7Usage_Percent_Desc',	'VisioL7Usage_Percent_Desc',	
'OneNoteL7Usage_Percent_Desc',	'ExcelL7Usage_Percent_Desc',	'AccessL7Usage_Percent_Desc',	'LyncL7Usage_Percent_Desc',	'PowerPointL7Usage_Percent_Desc',	'ProjectL14Usage_Percent_Desc',	
'PublisherL14Usage_Percent_Desc',	'OutlookL14Usage_Percent_Desc',	'WordL14Usage_Percent_Desc',	'VisioL14Usage_Percent_Desc',	'OneNoteL14Usage_Percent_Desc',	'ExcelL14Usage_Percent_Desc',	
'AccessL14Usage_Percent_Desc',	'LyncL14Usage_Percent_Desc',	'PowerPointL14Usage_Percent_Desc',	'ProjectL28Usage_Percent_Desc',	'PublisherL28Usage_Percent_Desc',	'OutlookL28Usage_Percent_Desc',
'WordL28Usage_Percent_Desc',	'VisioL28Usage_Percent_Desc',	'OneNoteL28Usage_Percent_Desc',	'ExcelL28Usage_Percent_Desc',	'AccessL28Usage_Percent_Desc',	'LyncL28Usage_Percent_Desc',	
'PowerPointL28Usage_Percent_Desc',	'Win10L7Usage_Percent_Desc',	'WinOtherL7Usage_Percent_Desc',	'MacL7Usage_Percent_Desc',	'iOSL7Usage_Percent_Desc',	'IPhoneL7Usage_Percent_Desc',	
'AndroidL7Usage_Percent_Desc',	'Win10L14Usage_Percent_Desc',	'WinOtherL14Usage_Percent_Desc',	'MacL14Usage_Percent_Desc',	'iOSL14Usage_Percent_Desc',	'IPhoneL14Usage_Percent_Desc',	
'AndroidL14Usage_Percent_Desc',	'Win10L28Usage_Percent_Desc',	'WinOtherL28Usage_Percent_Desc',	'MacL28Usage_Percent_Desc',	'iOSL28Usage_Percent_Desc',	'IPhoneL28Usage_Percent_Desc',	
'AndroidL28Usage_Percent_Desc',	'LatestSATType',	'CompositeSATType', 'CountryCode','Region','O365TenantReleaseTrack'
]

for x in StrTo_Cat:
    #print(x)
    data.loc[data[x].isnull(), x] ='No_Data'
    data[x]= data[x].astype('category')
    #pd.data[StrTo_Cat]= data[StrTo_Cat].astype('category')

#data['CompositeSATType']=data['CompositeSATType'].str.split('\r', expand=True)
data.loc[data['CompositeSATType'].isnull(), 'CompositeSATType'] ='No_SAT'
 
data=data.loc[data['CPTenant_ChannelName'].str.contains('DIRECT') | data['CPTenant_ChannelName'].str.contains('VL')|data['CPTenant_ChannelName'].str.contains('GODADDY.COM, LLC_EE83E790-62B7-4CCB-A1B6-26CAC3C77A8C')| data['CPTenant_ChannelName'].str.contains('RESELLER')]
#print(data['Det'].head())
data['Det']= data['Det'].astype(int)
data['Det']= data['Det'].astype('category') 

data = pd.DataFrame(data)
for x in StrTo_Cat:
        data = pd.concat([data, pd.get_dummies(data[x], prefix=x, prefix_sep='_',)], axis = 1)
        #drop the original column
        del data[x]

for x in Emptyto_Zero:
    data.loc[data[x].isnull(), x] =0
    data[x]=pd.to_numeric(data[x])

df_log = pd.DataFrame(data)
for c in Emptyto_Zero:
    df_log[c]=df_log[c].apply(lambda x:0 if x <= 0 else math.log(x))
    
#Min MAx after taking log

df_logMM = pd.DataFrame(df_log)

scaler = preprocessing.MinMaxScaler()
for c in Emptyto_Zero:
  #  print(c)
    df_logMM[c] = scaler.fit_transform(df_logMM[c].values.reshape(-1, 1))

df= df_logMM
y = df.Det_1
X=df.drop('Det_0',  axis=1)
X=X.drop('Det_1',  axis=1)
#X = X.loc[:,~X.columns.str.contains('SATType')] #possible data leakage

X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.1)

from sklearn.linear_model import LogisticRegression

# 2. instantiate model
logreg = LogisticRegression()

# 3. fit 
logreg.fit(X_train, Y_train)

new_pred_class = logreg.predict(X_test) #replace with new test tsv dataset
#Pdata = pd.read_csv("/data/DetractorPrediction/Deta1_pred.csv",index_col=None)
#new_pred_class = logreg.predict(Pdata)

columns_new = ['Predicted']
pred=pd.DataFrame(new_pred_class, columns=columns_new)

Check= pd.concat([pd.DataFrame(Y_test).reset_index(), pred], axis = 1)
#pdf= Predoutput.pred.value_counts()
#print(pdf)
#Check=Log_MM_Output
Log_MM_Output=pd.crosstab(Check.Det_1, Check.Predicted)
#Log_MM_Output.to_csv("DetractorPrediction_Check1.csv")
print(Log_MM_Output)

# new_data = pd.read_csv("/data/DetractorPrediction/FullScoringData_Split1.csv")
# new_data = new_data.head(100)
# score_tenants = new_data.TenantId
# feature_col = X_train.columns
# score_X_test = new_data.loc[:,feature_col]
# print(np.all(np.isfinite(score_X_test)))
# print(np.any(np.isnan(score_X_test)))
# score_X_test = score_X_test.dropna() #input contains either NaN, infinity, or a value too large
# print(score_X_test.shape)
# score_X_test.to_csv("/data/DetractorPrediction/Full_ScoringData_Head1.csv")
# score_pred_class = logreg.predict(score_X_test)
# score_pred = pd.DataFrame(score_pred_class, columns=columns_new)
# #score_tenants = pd.read_csv("/data/DetractorPrediction/FullScoringData_Split1TenantId.csv")
# Score= pd.concat([score_tenants.reset_index(), score_pred], axis = 1)
# Detractors = Score[Score.score_pred==1]
# Detractors.to_csv("DetractorPrediction_Check1.csv")

### TEST DATA ###

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

StrTo_Cat=['TenantState',	'TenantType',	'HasEducation',	'HasCharity',	'HasGovernment',	'HasExchange',	'HasLync',	'HasSharePoint',	'HasProPlus',	'HasYammer',	'HasSubscription',	'HasProject',	
'HasPaid',	'HasVisio',	'HasTrial',	'IsConcierge',	'DirectoryExtensionsSyncEnabled',	'DirSyncEnabled',	'PasswordSyncEnabled',	'PasswordWriteBackEnabled',	'IsDonMT',	'IsViral',	'IsTest',	'IsQuickStart',	
'IsFastTrackTenant',	'HasSKUE3',	'HasSKUE5',	'SPOEnvironment',	'IsRestrictRmsViralSignUp',	'IsMSODSDeleted','CommunicationLanguage',	'ConciergeInfoIsManualAdmittance',	'CPTenant_ChannelName',	
'Age_at_First_Ticket_Bucket',	'ProjectL7Usage_Percent_Desc',	'PublisherL7Usage_Percent_Desc',	'OutlookL7Usage_Percent_Desc',	'WordL7Usage_Percent_Desc',	'VisioL7Usage_Percent_Desc',	
'OneNoteL7Usage_Percent_Desc',	'ExcelL7Usage_Percent_Desc',	'AccessL7Usage_Percent_Desc',	'LyncL7Usage_Percent_Desc',	'PowerPointL7Usage_Percent_Desc',	'ProjectL14Usage_Percent_Desc',	
'PublisherL14Usage_Percent_Desc',	'OutlookL14Usage_Percent_Desc',	'WordL14Usage_Percent_Desc',	'VisioL14Usage_Percent_Desc',	'OneNoteL14Usage_Percent_Desc',	'ExcelL14Usage_Percent_Desc',	
'AccessL14Usage_Percent_Desc',	'LyncL14Usage_Percent_Desc',	'PowerPointL14Usage_Percent_Desc',	'ProjectL28Usage_Percent_Desc',	'PublisherL28Usage_Percent_Desc',	'OutlookL28Usage_Percent_Desc',
'WordL28Usage_Percent_Desc',	'VisioL28Usage_Percent_Desc',	'OneNoteL28Usage_Percent_Desc',	'ExcelL28Usage_Percent_Desc',	'AccessL28Usage_Percent_Desc',	'LyncL28Usage_Percent_Desc',	
'PowerPointL28Usage_Percent_Desc',	'Win10L7Usage_Percent_Desc',	'WinOtherL7Usage_Percent_Desc',	'MacL7Usage_Percent_Desc',	'iOSL7Usage_Percent_Desc',	'IPhoneL7Usage_Percent_Desc',	
'AndroidL7Usage_Percent_Desc',	'Win10L14Usage_Percent_Desc',	'WinOtherL14Usage_Percent_Desc',	'MacL14Usage_Percent_Desc',	'iOSL14Usage_Percent_Desc',	'IPhoneL14Usage_Percent_Desc',	
'AndroidL14Usage_Percent_Desc',	'Win10L28Usage_Percent_Desc',	'WinOtherL28Usage_Percent_Desc',	'MacL28Usage_Percent_Desc',	'iOSL28Usage_Percent_Desc',	'IPhoneL28Usage_Percent_Desc',	
'AndroidL28Usage_Percent_Desc',	'LatestSATType',	'CompositeSATType', 'CountryCode','Region','O365TenantReleaseTrack',#'DefaultDataLocation'
]

Emptyto_Zero=[ 'EXOEnabledUsers',	'LicensedUsers',	'LYOEnabledUsers',	'SPOEnabledUsers',	'OD4BEnabledUsers',	'ProPlusEnabledUsers',	'TotalUsers',	'EDUSubscriptionsCount',	
'ExchangeSubscriptionsCount',	'LyncSubscriptionsCount',	'PaidSubscriptionCount',	'ProjectSubscriptionsCount',	'SharePointSubscriptionsCount',	'TotalSubscriptionCount',	
'TrialSubscriptionCount',	'VisioSubscriptionsCount',	'SubscriptionCount',	'FirstTicketAge',	'TotalTickets',	'MinCloseTime',	'MaxCloseTime',	'MedCloseTime',	'DaysToSubEndDate',
'DaysTicketOpenAfterSubStart',	'TicketsFeedRecency',	'NumberOfTickets_Past7Days',	'NumberOfTickets_Past28Days',	'NumberOfTickets_Past90Days',	'NumberOfTickets_Past180Days',
'NumberOfTickets_Past360Days',	'PercentOfTickets',	'TotalTickets1Year',	'Exchange_unable_to_connect_sync_with_exchange',	'SharePoint_manage_sites_documents_and_lists',
'Admin_Get_reports_insights_and_usage_patterns_for_my_Office_365_tenant',	'Office_Client_Use_Office_apps_including_Mac',	'Admin_Find_and_signup_for_the_correct_Office_365_plan',	
'Dynamics_CRM_Setup_and_use_Dynamics_CRM_and_Parature_services',	'Intune_Download_Setup_and_Use_Intune',	'OneDrive_Setup_OneDrive_and_sync_my_documents',	'Admin_Sign_in_and_password_issues',
'Mobile_Connect_and_configure_mobile_devices',	'Setup_and_use_Stream',	'Exchange_Use_calendar_free_busy_and_contacts',	'Install_setup_and_use_Power_Bi',	
'Commerce_Manage_bills_payments_subscriptions_and_licenses',	'Exchange_Migrate_my_data_to_Office_365',	'Office_Client_Download_install_and_activate_Office_apps_including_Mac',	'Other',	
'Setup_and_use_OneNote',	'Exchange_Setup_and_manage_mailbox_exchange_online',	'Setup_and_use_PowerPoint',	'Exchange_Enable_hybrid_capabilities',	
'Setup_compliance_features_like_Archive_Retention_Litigation_eDiscovery_and_MDM',	'Admin_Setup_domain_and_DNS_settings_for_Office_365',	'Exchange_Use_OWA_Outlook_Web_App',	
'Outlook_Setup_and_use_Outlook_including_Mac',	'Office_Client_Word',	'Yammer_Setup_and_use_Yammer_services',	'Office_Client_Excel',	'Prevent_user_accounts_from_getting_compromised',	
'Send_and_receive_mail_on_time',	'Admin_Global_Office_365_setup_and_administration_DirSync_ADFS_Global_Exchange_settings',	'Admin_Manage_my_users_groups_and_resources',	'Setup_and_use_Project',	
'Skype_Setup_and_use_Skype_services',	'Keep_mailboxes_free_of_spam_and_viruses',	'Project_and_Planner_manage_projects_and_plans',	'Setup_and_use_Delve_Analytics',	
'Teams_Download_Setup_and_Use_Microsoft_Teams',	'L7TotalUsage',	'L14TotalUsage',	'L28TotalUsage',	'ProjectL7Usage',	'PublisherL7Usage',	'OutlookL7Usage',	'WordL7Usage',	
'VisioL7Usage',	'OneNoteL7Usage',	'ExcelL7Usage',	'AccessL7Usage',	'LyncL7Usage',	'PowerPointL7Usage',	'ProjectL14Usage',	'PublisherL14Usage',	'OutlookL14Usage',	'WordL14Usage',	
'VisioL14Usage',	'OneNoteL14Usage',	'ExcelL14Usage',	'AccessL14Usage',	'LyncL14Usage',	'PowerPointL14Usage',	'ProjectL28Usage',	'PublisherL28Usage',	'OutlookL28Usage',	'WordL28Usage',	
'VisioL28Usage',	'OneNoteL28Usage',	'ExcelL28Usage',	'AccessL28Usage',	'LyncL28Usage',	'PowerPointL28Usage',	'ProjectL7Usage_Percent',	'PublisherL7Usage_Percent',	'OutlookL7Usage_Percent',	
'WordL7Usage_Percent',	'VisioL7Usage_Percent',	'OneNoteL7Usage_Percent',	'ExcelL7Usage_Percent',	'AccessL7Usage_Percent',	'LyncL7Usage_Percent',	'PowerPointL7Usage_Percent',	'ProjectL14Usage_Percent',	
'PublisherL14Usage_Percent',	'OutlookL14Usage_Percent',	'WordL14Usage_Percent',	'VisioL14Usage_Percent',	'OneNoteL14Usage_Percent',	'ExcelL14Usage_Percent',	'AccessL14Usage_Percent',	
'LyncL14Usage_Percent',	'PowerPointL14Usage_Percent',	'ProjectL28Usage_Percent',	'PublisherL28Usage_Percent',	'OutlookL28Usage_Percent',	'WordL28Usage_Percent',	'VisioL28Usage_Percent',	
'OneNoteL28Usage_Percent',	'ExcelL28Usage_Percent',	'AccessL28Usage_Percent',	'LyncL28Usage_Percent',	'PowerPointL28Usage_Percent',	'Win10L7Usage',	'WinOtherL7Usage',	'MacL7Usage',	'iOSL7Usage',	
'IPhoneL7Usage',	'AndroidL7Usage',	'Win10L14Usage',	'WinOtherL14Usage',	'MacL14Usage',	'iOSL14Usage',	'IPhoneL14Usage',	'AndroidL14Usage',	'Win10L28Usage',	'WinOtherL28Usage',	'MacL28Usage',	
'iOSL28Usage',	'IPhoneL28Usage',	'AndroidL28Usage',	'Win10L7Usage_Percent',	'WinOtherL7Usage_Percent',	'MacL7Usage_Percent',	'iOSL7Usage_Percent',	'IPhoneL7Usage_Percent',	'AndroidL7Usage_Percent',	
'Win10L14Usage_Percent',	'WinOtherL14Usage_Percent',	'MacL14Usage_Percent',	'iOSL14Usage_Percent',	'IPhoneL14Usage_Percent',	'AndroidL14Usage_Percent',	'Win10L28Usage_Percent',	'WinOtherL28Usage_Percent',	
'MacL28Usage_Percent',	'iOSL28Usage_Percent',	'IPhoneL28Usage_Percent',	'AndroidL28Usage_Percent',	'TotalFeedbacks',	'LatestSATScore',	'CompositeSATScore','CommerceTenantTagsCount', 
'AssignedPlanCount','PartnerTenantCount','TotalGroupCount','DomainCount'
]

data = pd.read_csv("/data/DetractorPrediction/NPSPredictiveModelAttributesTenant_Split_3.csv",sep=',', header=None, names=Col_s, index_col=False)

print('data tenant count: ',data.TenantId.nunique(), data.shape)

data_s=data.drop(['CommunicationCulture',	'CreatedDate',	'CreateDateOfFirstSubscription',	'PurchaseDateOfFirstNonTrialSubscription',	
'CommerceTenantTagsArray','AssignedPlanServiceTypeArray','ConciergeInfoIsConceirge',	'ConciergeInfoProgramId',	
'CompanyLastDirSyncTime',	'PasswordSyncTime','CurrentDefaultDomain','SnapshotTime',	'MSODSTenant_CompanyTags',	
'MSODSTenant_ExtensionAttributes',	'MSODSTenant_DirSyncEnabled',	'FirstTicketDate',	'LastTicketDate',	'SubscriptionEndDate',	'SubscriptionStartDate',	'Percent_Of_Tickets_Bucket',	
'LatestFeedbackDateTime',	'Name',	'Country',	'City', 'LastCol', 'DefaultDataLocation'],axis=1)

data_s=data_s.loc[data_s['CPTenant_ChannelName'].str.contains('DIRECT') | data_s['CPTenant_ChannelName'].str.contains('VL')|data_s['CPTenant_ChannelName'].str.contains('GODADDY.COM, LLC_EE83E790-62B7-4CCB-A1B6-26CAC3C77A8C')| data_s['CPTenant_ChannelName'].str.contains('RESELLER') ]
tenant_num=data_s.loc[:,['TenantId']]

print(len(tenant_num))
print(tenant_num[:3])
print(tenant_num.index)

del data_s['TenantId']

# data_s.loc[data_s['LatestSATType'].isnull(), 'LatestSATType'] ='No_SAT'
# data_s.loc[data_s['LatestSATType']== '', 'LatestSATType'] ='No_SAT'
# data_s.loc[data_s['LatestSATType']== '#NULL#', 'LatestSATType'] ='No_SAT'
# data_s.loc[data_s['CompositeSATType']== '#NULL#', 'CompositeSATType'] ='No_SAT'
# data_s.loc[data_s['CompositeSATType']== '', 'CompositeSATType'] ='No_SAT'
# data_s.loc[data_s['CompositeSATType'].isnull(), 'LatestSATType'] ='No_SAT'

for x in Emptyto_Zero:
    data_s.loc[data_s[x].isnull(), x] =0

for x in StrTo_Cat:
    print(x)
    data_s.loc[data_s[x].isnull(), x] ='No_Data'
    data_s[x]= data_s[x].astype('category')


# for x in StrTo_Cat:
#     print(x)
print('data_s tenant count: ', data_s.shape)

data_s[StrTo_Cat]= data_s[StrTo_Cat].apply(lambda x: x.cat.codes)


data_s = pd.DataFrame(data_s)
for i, x in enumerate(StrTo_Cat):
        data_s = pd.concat([data_s, pd.get_dummies(data_s[x], prefix=x, prefix_sep='_',)], axis = 1)
        #drop the original column
        del data_s[x]
        print(i,len(StrTo_Cat)+1)

#alt_null_fields = [] 
for x in Emptyto_Zero:
    print(x)
    if '#NULL#' in data_s[x].unique():
        #print(' has alt nulls')
        #alt_null_fields.append(x)
        data_s[x] = data_s[x].replace('#NULL#','0')
    data_s.loc[data_s[x].isnull(), x] =0
    data_s[x]=pd.to_numeric(data_s[x])

df_log_s = pd.DataFrame(data_s)
for c in Emptyto_Zero:
    df_log_s[c]=df_log_s[c].apply(lambda x:0 if x <= 0 else math.log(x))

print(df_log_s.shape)

df_logMM_s = pd.DataFrame(df_log_s)

scaler = preprocessing.MinMaxScaler()
for c in Emptyto_Zero:
    # print(c)
    df_logMM_s[c] = scaler.fit_transform(df_logMM_s[c].values.reshape(-1, 1))

df_logMM_s= pd.concat([tenant_num, df_logMM_s], axis=1)
print(df_logMM_s.shape)
#df_logMM_s.to_json("/data/DetractorPrediction/FullScoringData_Split1.json")
feature_col = X_train.columns

print(set(list(df_logMM_s))-set(list(df_log_s)))

new_data = df_logMM_s
score_X_test = new_data.loc[:,X_train.columns]

print(np.all(np.isfinite(score_X_test)))
print(np.any(np.isnan(score_X_test)))

score_X_test = score_X_test.fillna(0) #input contains either NaN, infinity, or a value too large
print(np.any(np.isnan(score_X_test)))

print(score_X_test.shape, X_train.shape)

score_pred_class = logreg.predict(score_X_test)

score_pred = pd.DataFrame(score_pred_class, columns=columns_new)

Score= pd.concat([tenant_num.reset_index(), score_pred], axis = 1)
print(Score[:5])
Detractors = Score[Score.Predicted==1].reset_index(drop=True)
print(Detractors[:5])
print(Score.shape,Detractors.shape)
Detractors.to_csv('/data/DetractorPrediction/TenantDetractors_Split3.csv')

#data_s = pd.concat([Score,data_s],axis=1)



