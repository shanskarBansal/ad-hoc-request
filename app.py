import streamlit as st
import streamlit.components.v1 as components

def app_first_block():
    import streamlit as st
    import requests
    import json
    import pandas as pd
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import pygsheets
    import numpy as np
    from datetime import datetime, timedelta
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import tempfile



    class google_api_class:
        def __init__(self, credential_file=None):
            """
            initializing google api class with parameters from configurations file
            :return: none
            """
            self.credentials = credential_file
            self.gapi_service = self.initialize_gapi_services()

        def initialize_gapi_services(self):
            """
            initializing google api services to connect the excel endpoint
            :return: api instance
            """
            SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
            SERVICE_ACCOUNT_FILE = self.credentials
            gapi_credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
            service = build("sheets", "v4", credentials=gapi_credentials)
            sheet = service.spreadsheets()
            return sheet

        def append_google_sheets(self, data=None, spreadsheet_id=None, sub_sheet_name=None, sheet_range=None):
            request = self.initialize_gapi_services().values().append(
                spreadsheetId=spreadsheet_id,
                range=sub_sheet_name + "!" + sheet_range,
                valueInputOption="USER_ENTERED",
                body={"values": data.values.tolist()},
            )
            request.execute()
            pass

        def read_sheet(self, sheet_range=None, sheet_name=None, sheet_id=None):
            """
            This function is used to get data from a google sheet
            :param sheet_id: sheet_id of the google spreadsheet
            :param sheet_range: range from which data is to be read
            :param sheet_name: name of the sub sheet in the google sheet from where data is to be read
            :return: list of lists
            """
            request = (
                self.gapi_service.values()
                    .get(spreadsheetId=sheet_id, range=sheet_name + "!" + sheet_range)
                    .execute()
            )
            values = request.get("values", [])
            resp_df = pd.DataFrame(data=values, columns=values[0])
            resp_df = resp_df.iloc[1:, :]
            return resp_df
            
        def write_to_gsheet(self, spreadsheet_id=None, sheet_name=None, data_df=None):
            """
            this function takes data_df and writes it under spreadsheet_id
            and sheet_name using your credentials under service_file_path
            """
            service_file_path = self.credentials
            gc = pygsheets.authorize(service_file=service_file_path)
            sh = gc.open_by_key(spreadsheet_id)
            print(sh)
            try:
                sh.add_worksheet(sheet_name)
                print("Success")
            except:
                pass
            wks_write = sh.worksheet_by_title(sheet_name)
            wks_write.clear("A1", None, "*")
            wks_write.set_dataframe(data_df, (1, 1), encoding="utf-8", fit=True)
            wks_write.frozen_rows = 1


    Google_api_credential_file = dict(st.secrets["gcp_service_account"])
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        temp_file.write(json.dumps(Google_api_credential_file).encode())
        temp_file_path = temp_file.name
    google_api_object = google_api_class(credential_file=temp_file_path)


    api_key = st.secrets["auths_tokens"]

    today = datetime.today().strftime('%Y-%m-%d')
    timestamp = datetime.today() - timedelta(days=1)
    yesterday = timestamp.strftime('%Y-%m-%d')


    merge_keys = ["Name", "Names", "Page Level", "State", "Party","Page Type"]

    def input_df(sheet_name=None, sheet_id=None):
        profile_df = google_api_object.read_sheet(sheet_range='A1:AZ30000', sheet_name=sheet_name, sheet_id=sheet_id)

        profile_df_list = profile_df[["Instagram Link", "Profile/Page Name", "Page Level", "Page Type", "State", "Party"]].values.tolist()

        final_profile_type = []
        final_profile_link = []
        final_profile_name = []
        final_profile_page_level = []
        final_profile_state = []
        final_profile_party = []

        for count, cand_profile in enumerate(profile_df_list):
            try:
    #             if count == 5:
    #                 break
                profile_link = cand_profile[0].split('instagram.com/')
                profile = profile_link[1].split('/')
                final_profile_link.append(profile[0])
                final_profile_name.append(cand_profile[1])
                final_profile_type.append(cand_profile[3])
                final_profile_page_level.append(cand_profile[2])
                final_profile_state.append(cand_profile[4])
                final_profile_party.append(cand_profile[5])
            except Exception:
                pass
        profile_type_df = pd.DataFrame(
            np.column_stack([final_profile_link, final_profile_name, final_profile_page_level, final_profile_type, final_profile_state, final_profile_party]),
            columns=['Username', 'Names', 'Page Level', 'Page Type', 'State', 'Party'])  # Updated column order
        return final_profile_link, profile_type_df, final_profile_name

    def response_df(ds_acc_list=None, fields_list=None, api_key=None, start_date=None, end_date=None, profile_type_df=None):
        final_df = pd.DataFrame()
        available_list = []
        not_available_list = []
        fields = fields_list.split(',')

        for count, each_acc in enumerate(ds_acc_list):
            try:
                st.write(each_acc)
                payload = {"ds_id": "IGPD2",
                        "ds_accounts": each_acc,
                        "ds_user": "111966744882904",
                        "start_date": start_date,
                        "end_date": end_date,
                        "fields": fields,
                        "max_rows": 100000,
                        "api_key": api_key}
                payload = json.dumps(payload)
                url = "https://api.supermetrics.com/enterprise/v2/query/data/json?json=" + payload
                response = requests.get(url)
                print(response)
                if response:
                    available_list.append(each_acc)
                else:
                    not_available_list.append(each_acc)

                data = response.json()

                data_response = data["data"]

                df = pd.DataFrame(data_response)

                df.columns = df.iloc[0]
                df.drop(df.index[0], inplace=True)
                df['Names'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Names'].values[0]
                df['Page Level'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Page Level'].values[0]
                df['State'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'State'].values[0]
                df['Party'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Party'].values[0]
                df['Page Type'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Page Type'].values[0]

                final_df = pd.concat([final_df, df], ignore_index=True)


            except Exception as e:
                print(str(e))
        return final_df, available_list, not_available_list


    from datetime import timedelta
    import pandas as pd


    def final_SM_report_df(ds_acc_list=None, api_key=None, start_date=None, end_date=None, profile_type_df=None):
        try:
            fields_list = "ig_id,post_id,post_comments,post_type,post_likes,post_caption,post_timestamp,post_media_url,post_permalink,username,name,likes_per_post,comments_per_post"

            final_df, available_list, not_available_list = response_df(ds_acc_list=ds_acc_list, fields_list=fields_list,
                                                                    api_key=api_key, start_date=start_date,
                                                                    end_date=end_date, profile_type_df=profile_type_df)
            print("final_df columns---", final_df.columns)
            

            final_df['Post created'] = pd.to_datetime(final_df['Post created'])
            add_time = timedelta(hours=3, minutes=30)
            final_df['Post created'] = final_df['Post created'] + add_time

            fields_list = "name,followers"
            followers_df, available_list, not_available_list = response_df(ds_acc_list=ds_acc_list, fields_list=fields_list,
                                                                        api_key=api_key, start_date=start_date,
                                                                        end_date=end_date, profile_type_df=profile_type_df)
            print("followers_df created")
            print("followers_df columns =", followers_df.columns)

            mask = (final_df['Post created'] >= start_date + ' 00:00:00') & (final_df['Post created'] <= end_date + ' 00:00:00')
            final_df = final_df.loc[mask]

            df_final = pd.merge(final_df, followers_df, on=merge_keys, how='outer')
            df_final['Post Count'] = 1
            df_final.loc[df_final['Post ID'].isnull(), 'Post Count'] = 0


            for column in ['Likes', 'Likes per post', 'Comments per post', 'Profile followers']:
                df_final[column] = pd.to_numeric(df_final[column])

            print("df_final columns =", df_final.columns)
        
            raw_data_df = df_final
        #     raw_data_df = final_df.copy()
            raw_data_df.fillna(0, inplace=True)
            raw_data_df.replace('', 0, inplace=True)
            string_cols = ['Name', 'Names']
            for col in string_cols:
                raw_data_df[col].fillna('', inplace=True)

            raw_data_df['Interaction per post'] = pd.to_numeric(raw_data_df['Likes'] + raw_data_df['Comments'])
            raw_data_df['Engagement per post'] = pd.to_numeric(
                (raw_data_df['Interaction per post'] / raw_data_df['Profile followers']) * 100)
            raw_data_df['Engagement per post'] = raw_data_df['Engagement per post'].round(2)

        #     prof_type_df = df_final[['Name', 'Names', 'Page Type', 'Page Level', 'State', 'Party']]

        #     print("df_final column names before grouping =", df_final.columns)
            df1 = df_final.groupby(['Name', 'Names', "Page Type", "Page Level", "State", "Party", 'Profile followers'], as_index=False)[['Post Count', 'Likes per post', 'Comments per post']].apply(lambda x: x.astype(int).sum())
            
            df1.rename(columns={'Likes per post': 'Total Likes', 'Comments per post': 'Total Comments'}, inplace=True)
            cols = df_final.select_dtypes(include='object').columns
            df_final[cols] = df_final[cols].apply(pd.to_numeric, errors='coerce')
            df_final.fillna(0, inplace=True)
            df2 = df_final.groupby(['Name', 'Names', "Page Type", "Page Level", "State", "Party"], as_index=False).mean()
            df2 = df2[['Name', 'Names', "Page Type", "Page Level", "State", "Party", 'Profile followers', 'Likes per post', 'Comments per post']]

            for key in merge_keys:
                df1[key] = df1[key].astype(str)
                df2[key] = df2[key].astype(str)
                

            final_sm_df = pd.merge(df1, df2, on=['Name', 'Names', "Page Type", "Page Level", "State", "Party", 'Profile followers'], how='outer')
            raw_data_df_unprocessed = final_df.copy()

            
            return final_sm_df, available_list, not_available_list, raw_data_df_unprocessed
        except Exception as e:
            print(f"Error in final_SM_report_df: {str(e)}")
            return pd.DataFrame(), [], [], pd.DataFrame()



    def fetch_data_in_chunks(sheet_name, sheet_id, start_date, end_date, api_key):
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            all_raw_data = []
            chunk_size = 10  # Number of days in each chunk
            
            while start_date < end_date:
                end_date_i = start_date + timedelta(days=chunk_size - 1)
                
                if end_date_i > end_date:
                    end_date_i = end_date
                    
                kr_final_profile_link, kr_profile_type_df, kr_final_profile_name = input_df(sheet_name=sheet_name, sheet_id=sheet_id)
                final_sm_df_chunk, _, _, kar_raw_data_df = final_SM_report_df(
                    ds_acc_list=kr_final_profile_link,
                    api_key=api_key,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date_i.strftime('%Y-%m-%d'),
                    profile_type_df=kr_profile_type_df
                )
                
                if final_sm_df_chunk.empty or kar_raw_data_df.empty:
                    print(f"No data available for the period {start_date.strftime('%Y-%m-%d')} to {end_date_i.strftime('%Y-%m-%d')}")
                else:
                    print("gaya")
                    merged_df_i = pd.merge(
                        kar_raw_data_df, 
                        final_sm_df_chunk[['Name', 'Names', 'Page Type', 'Page Level', 'State', 'Party', 'Profile followers']], 
                        on=['Name', 'Names', 'Page Type', 'Page Level', 'State', 'Party'], 
                        how='left'
                    )
                    all_raw_data.append(merged_df_i)
                
                start_date = end_date_i + timedelta(days=1)

            kar_comm_data_df = pd.concat(all_raw_data, ignore_index=True)
        
        except Exception as e:
            print(f"Error fetching or processing data: {str(e)}")
            kar_comm_data_df = pd.DataFrame()  # Return an empty DataFrame or handle as needed
        
        return kar_comm_data_df

    # kar_sheet_name = 'Sheet1'  
    # kar_sheet_id = '1KEzue5dhKj6aNhoiD_wFpWi9Z5yuPCkanSYj2Cenxzs'  
    # start_date = '2024-04-30'  
    # end_date = '2024-06-01'  

    # kar_comm_data_df = fetch_data_in_chunks(kar_sheet_name, kar_sheet_id, start_date, end_date, api_key)


    def get_gspread_client():
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        return client
                   
    def get_sheet_data(sheet_id):
        client = get_gspread_client()
        sheet = client.open_by_key(sheet_id)
        return sheet
        
    st.title("INSTAGRAM SOCIAL MEDIA REQUEST")

    gsheet_name = st.text_input("Enter Google Sheet ID")
    if gsheet_name:
        sheet = get_sheet_data(gsheet_name)
        sheet_names = [worksheet.title for worksheet in sheet.worksheets()] 

    if gsheet_name:
        sheet_name = st.selectbox("Select Sheet Name", options=sheet_names)

        today_date = datetime.today().date()
        start_date = str(st.date_input("Start Date", value=today_date))
        end_date = str(st.date_input("End Date", value=today_date) + timedelta(days=1))

        if st.button("Fetch Data"):
            kar_comm_data_df = fetch_data_in_chunks(sheet_name,gsheet_name, start_date, end_date, api_key)  
            if kar_comm_data_df.empty:
                st.write("No data available for the period")
            else:         
                kar_comm_data_df['Posts'] = 1
                kar_comm_data_df['Likes'] = pd.to_numeric(kar_comm_data_df['Likes'], errors='coerce').fillna(0)
                kar_comm_data_df['Comments'] = pd.to_numeric(kar_comm_data_df['Comments'], errors='coerce').fillna(0)
                kar_comm_data_df['Profile followers'] = pd.to_numeric(kar_comm_data_df['Profile followers'], errors='coerce').fillna(0)


                kar_comm_data_df['Interation'] = kar_comm_data_df['Likes'] + kar_comm_data_df['Comments']


                kar_comm_data_df['Enagagement'] = kar_comm_data_df['Interation'] / kar_comm_data_df['Profile followers']

                google_api_object.write_to_gsheet(spreadsheet_id=gsheet_name,sheet_name="insta_raw_data",data_df=kar_comm_data_df)
                st.write("INSTA RAW DATA SHEET IS CREATED")   
                kar_comm_data_df['Posts'] = pd.to_numeric(kar_comm_data_df['Posts'], errors='coerce').fillna(0)
                kar_comm_data_df['Likes'] = pd.to_numeric(kar_comm_data_df['Likes'], errors='coerce').fillna(0)
                kar_comm_data_df['Comments'] = pd.to_numeric(kar_comm_data_df['Comments'], errors='coerce').fillna(0)

                kar_comm_data_df['Profile followers'] = pd.to_numeric(kar_comm_data_df['Profile followers'], errors='coerce').fillna(0)

                aggregated_data = kar_comm_data_df.groupby('Name').agg({
                    'Page Level': 'first',
                    'Page Type': 'first',
                    'State': 'first',
                    'Party': 'first',
                    'Profile followers': 'first',
                    'Posts': 'sum',
                    'Likes': 'sum',
                    'Comments': 'sum',
                }).reset_index()

                aggregated_data['Likes Per Post'] = aggregated_data['Likes'] / aggregated_data['Posts'].replace(0, 1)
                aggregated_data['Comments Per Post'] = aggregated_data['Comments'] / aggregated_data['Posts'].replace(0, 1)
                aggregated_data['Total Interactions'] = aggregated_data['Likes'] + aggregated_data['Comments']
                aggregated_data['Avg Interactions'] = aggregated_data['Total Interactions'] / aggregated_data['Posts'].replace(0, 1)
                aggregated_data['Total Engagement'] = aggregated_data['Total Interactions'] / aggregated_data['Profile followers'].replace(0, 1)
                aggregated_data['Avg Engagement'] = aggregated_data['Total Engagement'] / aggregated_data['Posts'].replace(0, 1)
                aggregated_data['Date'] = f"{start_date} 12:00 AM - {end_date} 12:00 AM"

                final_aggr_data = aggregated_data.rename(columns={
                    'Posts': 'Total Posts',
                    'Likes': 'Total Likes',
                    'Comments': 'Total Comments',
                })
                google_api_object.write_to_gsheet(spreadsheet_id=gsheet_name,sheet_name="insta_agg_data",data_df=final_aggr_data)
                st.write("INSTA AGG DATA SHEET IS CREATED")  
    else:
        st.write("Please Enter a Valid Google Sheet_Id")


def app_second_block():
    import streamlit as st
    import requests
    import json
    import pandas as pd
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    import pygsheets
    import numpy as np
    from datetime import datetime, timedelta
    import gspread
    import tempfile
    from oauth2client.service_account import ServiceAccountCredentials

    class google_api_class:
        def __init__(self, credential_file=None):
            """
            initializing google api class with parameters from configurations file
            :return: none
            """
            self.credentials = credential_file
            self.gapi_service = self.initialize_gapi_services()

        def initialize_gapi_services(self):
            """
            initializing google api services to connect the excel endpoint
            :return: api instance
            """
            SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
            SERVICE_ACCOUNT_FILE = self.credentials
            gapi_credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
            service = build("sheets", "v4", credentials=gapi_credentials)
            sheet = service.spreadsheets()
            return sheet

        def append_google_sheets(self, data=None, spreadsheet_id=None, sub_sheet_name=None, sheet_range=None):
            request = self.initialize_gapi_services().values().append(
                spreadsheetId=spreadsheet_id,
                range=sub_sheet_name + "!" + sheet_range,
                valueInputOption="USER_ENTERED",
                body={"values": data.values.tolist()},
            )
            request.execute()
            pass

        def read_sheet(self, sheet_range=None, sheet_name=None, sheet_id=None):
            """
            This function is used to get data from a google sheet
            :param sheet_id: sheet_id of the google spreadsheet
            :param sheet_range: range from which data is to be read
            :param sheet_name: name of the sub sheet in the google sheet from where data is to be read
            :return: list of lists
            """
            request = (
                self.gapi_service.values()
                    .get(spreadsheetId=sheet_id, range=sheet_name + "!" + sheet_range)
                    .execute()
            )
            values = request.get("values", [])
            resp_df = pd.DataFrame(data=values, columns=values[0])
            resp_df = resp_df.iloc[1:, :]
            return resp_df
            
        def write_to_gsheet(self, spreadsheet_id=None, sheet_name=None, data_df=None):
            """
            this function takes data_df and writes it under spreadsheet_id
            and sheet_name using your credentials under service_file_path
            """
            service_file_path = self.credentials
            gc = pygsheets.authorize(service_file=service_file_path)
            sh = gc.open_by_key(spreadsheet_id)
            print(sh)
            try:
                sh.add_worksheet(sheet_name)
                print("Success")
            except:
                pass
            wks_write = sh.worksheet_by_title(sheet_name)
            wks_write.clear("A1", None, "*")
            wks_write.set_dataframe(data_df, (1, 1), encoding="utf-8", fit=True)
            wks_write.frozen_rows = 1

    Google_api_credential_file = dict(st.secrets["gcp_service_account"])

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        temp_file.write(json.dumps(Google_api_credential_file).encode())
        temp_file_path = temp_file.name
    google_api_object = google_api_class(credential_file=temp_file_path)

    api_key = st.secrets["auths_tokens"]

    today = datetime.today().strftime('%Y-%m-%d')
    timestamp = datetime.today() - timedelta(days=1)
    yesterday = timestamp.strftime('%Y-%m-%d')

    def input_df(sheet_name=None, sheet_id=None):
        profile_df = google_api_object.read_sheet(sheet_range='A1:AZ30000', sheet_name=sheet_name, sheet_id=sheet_id)

        profile_df_list = profile_df[["Facebook Link", "Profile/Page Name",	"Page Level",	"Page Type",	"State",	"Party", "AC"]].values.tolist()
        print(profile_df_list)

        final_profile_type = []
        final_profile_link = []
        final_profile_name = []
        final_profile_page_level = []
        final_profile_state = []
        final_profile_party = []
        final_profile_ac = []

        for count, cand_profile in enumerate(profile_df_list):
            try:
    #             if count==5:
    #                 break
                profile_link = cand_profile[0].split('facebook.com/')
                
                profile = profile_link[1].split('/')
                
                final_profile_link.append(profile[0])
            
                final_profile_name.append(cand_profile[1])
                final_profile_type.append(cand_profile[3])
                final_profile_page_level.append(cand_profile[2])
                final_profile_state.append(cand_profile[4])
                final_profile_party.append(cand_profile[5])
                final_profile_ac.append(cand_profile[6])

            except Exception as e:
                print(str(e))
                # print(cand_profile)
        
        profile_type_df = pd.DataFrame(np.column_stack([ final_profile_link, final_profile_name,final_profile_page_level,final_profile_type,final_profile_state,final_profile_party, final_profile_ac]),
                                    columns=[ 'Username', 'Names',"Page Level",	"Page Type","State","Party", "AC"])
        return final_profile_link, profile_type_df,final_profile_name


    def response_df(ds_acc_list=None, fields_list=None, api_key=None, start_date=None, end_date=None, profile_type_df=None,
                    profile_name=None):
        final_df = pd.DataFrame()
        available_list = []
        not_available_list = []
        fields = fields_list.split(',')

        for count, each_acc in enumerate(ds_acc_list):

            try:
                st.write(each_acc)
                payload = {"ds_id": "FBPD",
                        "ds_accounts": each_acc,
                        "ds_user": "engineering@varaheanalytics.com",
                        "start_date": start_date,
                        "end_date": end_date,
                        "fields": fields,
                        "max_rows": 100000,
                        "api_key": api_key}
                payload = json.dumps(payload)
                url = "https://api.supermetrics.com/enterprise/v2/query/data/json?json=" + payload
                response = requests.get(url)
                print(response)

                if response:
                    available_list.append(each_acc)
                else:
                    not_available_list.append(each_acc)
                data = response.json()
                data_response = data["data"]

                df = pd.DataFrame(data_response)
                df.columns = df.iloc[0]
                df.drop(df.index[0], inplace=True)
                df['Name'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Names'].values[0]
                df['Page Level'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Page Level'].values[0]
                df['State'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'State'].values[0]
                df['Party'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Party'].values[0]
                df['Page Type'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'Page Type'].values[0]
                df['AC'] = profile_type_df.loc[(profile_type_df['Username'] == each_acc), 'AC'].values[0]
        

                final_df = pd.concat([final_df, df], ignore_index=True)



            except Exception as e:
                print(str(e))

        return final_df, available_list, not_available_list

    def final_SM_report_df(ds_acc_list=None, api_key=None, start_date=None, end_date=None, profile_type_df=None,
                        profile_name=None):
        fields_list = "post_id,name,likes_count,reactions_count,comments_count,shares_count,reactions_love,reactions_wow,reactions_haha,reactions_sad,reactions_angry,reactions_thankful,reactions_pride,created_time,permalink_url,story,full_picture,caption,message,place,source,post_link,application_name,application_link,application_category,story_tags,type,likesPerPost,reactionsPerPost,commentsPerPost,sharesPerPost"
        #     fields_list = "name,likes_count,reactions_count,comments_count,shares_count,reactions_love,reactions_wow,reactions_haha,reactions_sad,reactions_angry,reactions_thankful,reactions_pride,created_time,permalink_url,story,full_picture,caption,message,place,source,post_link,application_name,application_link,application_category,story_tags,type,likesPerPost,reactionsPerPost,commentsPerPost,sharesPerPost"
        final_df, available_list, not_available_list = response_df(ds_acc_list=ds_acc_list, fields_list=fields_list,
                                                                api_key=api_key, start_date=start_date,
                                                                end_date=end_date, profile_type_df=profile_type_df,
                                                                profile_name=profile_name)
        print(final_df)
        st.write(final_df)
        # created_time_col = final_df.get('Created time')
        final_df['Created time'] = pd.to_datetime(final_df['Created time'])
        add_time = timedelta(hours=3, minutes=30)
        final_df['Created time'] = final_df['Created time'] + add_time

        fields_list = "name,followers_count"
        followers_df, available_list, not_available_list = response_df(ds_acc_list=ds_acc_list, fields_list=fields_list,
                                                                    api_key=api_key, start_date=start_date,
                                                                    end_date=end_date, profile_type_df=profile_type_df,
                                                                    profile_name=profile_name)

        # new_df = followers_df.copy()
        # new_df['Date'] = end_date
        # print("new_df columns = ",new_df.columns)

        # new_df.drop(columns=['Name (Profile)'], inplace=True)
        # new_df.rename(columns={'Page followers': 'followers'}, inplace=True)
        # new_df = new_df[['Name', 'followers', 'Date']]

        mask = (final_df['Created time'] >= start_date + ' 00:00:00') & (final_df['Created time'] <= end_date + ' 00:00:00')
        df_final = final_df.loc[mask]
        
        

        df_final = pd.merge(final_df, followers_df, on=["Name (Profile)","Name","Page Level","Page Type","State","Party", "AC"], how='outer')
    #     print('df_final',df_final.dtypes)
        
        columns_to_convert = ['Likes', 'Reactions', 'Comments', 'Post shares', 'Likes per post', 
                            'Reactions per post', 'Comments per post', 'Shares per post']
        for column in columns_to_convert:
            df_final[column] = pd.to_numeric(df_final[column], errors='coerce')

        df_final['Post Count'] = 1
        df_final.loc[df_final['Post ID'].isnull(), 'Post Count'] = 0

        raw_data_df = df_final
        raw_data_df.fillna(0, inplace=True)
        raw_data_df.replace('', 0, inplace=True)
        raw_data_df['Interactions Per Post'] = pd.to_numeric(raw_data_df['Reactions']) + pd.to_numeric(
            raw_data_df['Comments']) + pd.to_numeric(raw_data_df['Post shares'])
        raw_data_df['Engagement Per Post'] = pd.to_numeric(
            (raw_data_df['Interactions Per Post'] / raw_data_df['Page followers']) * 100)
        raw_data_df['Engagement Per Post'] = raw_data_df['Engagement Per Post'].round(2)

        prof_type_df = df_final[['Name (Profile)',"Name","Page Level","Page Type","State","Party", "AC"]]

        df1 = df_final.groupby(['Name (Profile)',"Name","Page Level","Page Type","State","Party",'Page followers', "AC"], as_index=False)[
            ['Post Count', 'Likes per post', 'Comments per post', 'Shares per post', 'Reactions per post']].sum()

        df1.rename(columns={'Likes per post': 'Total Likes', 'Comments per post': 'Total Comments',
                            'Shares per post': 'Total Shares', 'Reactions per post': 'Total Reactions'}, inplace=True)

        grouping_cols = ['Name (Profile)', "Name", "Page Level", "Page Type", "State", "Party", "AC"]
        aggregating_cols = ['Likes per post', 'Comments per post', 'Shares per post', 'Reactions per post']

        df2 = df_final.groupby(grouping_cols, as_index=False)[aggregating_cols].mean()
        df2 = df2[
            ['Name (Profile)',"Name","Page Level","Page Type","State","Party", "AC",'Likes per post', 'Comments per post', 'Shares per post', 'Reactions per post']]
        final_sm_df = pd.merge(df1, df2, on=["Name (Profile)","Name","Page Level","Page Type","State","Party"], how='outer')

        final_sm_df['Interactions Per Post'] = pd.to_numeric(final_sm_df['Reactions per post']) + pd.to_numeric(
            final_sm_df['Comments per post']) + pd.to_numeric(final_sm_df['Shares per post'])

        final_sm_df['Shares/1000 Followers'] = pd.to_numeric(final_sm_df['Shares per post']) / (
                    final_sm_df['Page followers'] / 1000)
        final_sm_df['Reactions/1000 Followers'] = pd.to_numeric(final_sm_df['Reactions per post']) / (
                    final_sm_df['Page followers'] / 1000)
        final_sm_df['Total Interactions'] = pd.to_numeric(final_sm_df['Total Reactions']) + final_sm_df[
            'Total Comments'] + (final_sm_df['Total Shares'])
        final_sm_df['Avg Interactions'] = pd.to_numeric(final_sm_df['Reactions per post']) + final_sm_df[
            'Comments per post'] + (final_sm_df['Shares per post'])
        final_sm_df['Total Engagement'] = pd.to_numeric(
            (final_sm_df['Total Interactions'] / final_sm_df['Page followers']) * 100)
        final_sm_df['Avg Engagement'] = pd.to_numeric(
            (final_sm_df['Avg Interactions'] / final_sm_df['Page followers']) * 100)
        final_sm_df['Engagement Per Post'] = pd.to_numeric(
            (final_sm_df['Interactions Per Post'] / final_sm_df['Page followers']) * 100)

        date = start_date + ' 12:00 AM' + ' - ' + end_date + ' 12:00 AM'
        final_sm_df['Date'] = date
        final_sm_df = final_sm_df.round(2)
        final_sm_df = pd.merge(final_sm_df, prof_type_df, on=["Name (Profile)","Name","Page Level",	"Page Type","State","Party"], how='outer')

        final_sm_df.rename(columns={'Post Count': 'Posts'}, inplace=True)
        final_sm_df = final_sm_df.drop_duplicates()

        # Surrogate_df = final_sm_df.loc[final_sm_df['Profile Type'] == 'Surrogate']

        final_sm_df.drop("Name (Profile)", inplace=True, axis=1)
        # raw_data_df.drop("Name (Profile)", inplace=True, axis=1)

        return final_sm_df, available_list, not_available_list, raw_data_df

    def fetch_data_in_chunks(sheet_name, sheet_id, start_date, end_date, api_key):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        total_days = (end_date - start_date).days
        
        all_raw_data = []
        
        # Only split days if more than 15
        if total_days > 15:
            while start_date < end_date:
                end_date_i = start_date + timedelta(days=9)  # 10 days = 9 days + start day
                if end_date_i > end_date:
                    end_date_i = end_date
                    
                kr_final_profile_link, kr_profile_type_df, kr_final_profile_name = input_df(sheet_name=sheet_name, sheet_id=sheet_id)
                
                _, _, _, kar_raw_data_df_i = final_SM_report_df(
                    ds_acc_list=kr_final_profile_link,
                    api_key=api_key,
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date_i.strftime('%Y-%m-%d'),
                    profile_type_df=kr_profile_type_df
                )
                
                all_raw_data.append(kar_raw_data_df_i)
                start_date = end_date_i + timedelta(days=1)
        else:
            kr_final_profile_link, kr_profile_type_df, kr_final_profile_name = input_df(sheet_name=sheet_name, sheet_id=sheet_id)
            _, _, _, kar_raw_data_df = final_SM_report_df(
                ds_acc_list=kr_final_profile_link,
                api_key=api_key,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                profile_type_df=kr_profile_type_df
            )
            all_raw_data.append(kar_raw_data_df)
        
        kar_comm_data_df = pd.concat(all_raw_data, ignore_index=True)
        
        return kar_comm_data_df
        
    def get_gspread_client():
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        return client
            
    def get_sheet_data(sheet_id):
        client = get_gspread_client()
        sheet = client.open_by_key(sheet_id)
        return sheet
        
    st.title("FACEBOOK SOCIAL MEDIA REQUEST")

    gsheet_name = st.text_input("Enter Google Sheet ID")
    if gsheet_name:
        sheet = get_sheet_data(gsheet_name)
        sheet_names = [worksheet.title for worksheet in sheet.worksheets()] 

    if gsheet_name:
        sheet_name = st.selectbox("Select Sheet Name", options=sheet_names)

        today_date = datetime.today().date()
        start_date = str(st.date_input("Start Date", value=today_date))
        end_date = str(st.date_input("End Date", value=today_date))

        if st.button("Fetch Data"):
            kar_comm_data_df = fetch_data_in_chunks(sheet_name,gsheet_name, start_date, end_date, api_key)  
            if kar_comm_data_df.empty:
                st.write("No data available for the period")
            else:
                google_api_object.write_to_gsheet(spreadsheet_id= gsheet_name ,sheet_name="FB_raw_data",data_df=kar_comm_data_df)
                st.write("FACEBOOK RAW DATA SHEET  IS CREATED") 
                kar_comm_data_df['Post Count'] = pd.to_numeric(kar_comm_data_df['Post Count'], errors='coerce').fillna(0)
                kar_comm_data_df['Likes'] = pd.to_numeric(kar_comm_data_df['Likes'], errors='coerce').fillna(0)
                kar_comm_data_df['Comments'] = pd.to_numeric(kar_comm_data_df['Comments'], errors='coerce').fillna(0)
                kar_comm_data_df['Shares per post'] = pd.to_numeric(kar_comm_data_df['Shares per post'], errors='coerce').fillna(0)
                kar_comm_data_df['Reactions'] = pd.to_numeric(kar_comm_data_df['Reactions'], errors='coerce').fillna(0)
                kar_comm_data_df['Page followers'] = pd.to_numeric(kar_comm_data_df['Page followers'], errors='coerce').fillna(0)

                aggregated_data = kar_comm_data_df.groupby('Name').agg({
                    'Name (Profile)': 'first',
                    'Page Level': 'first',
                    'Page Type': 'first',
                    'State': 'first',
                    'AC': 'first',
                    'Party': 'first',
                    'Page followers': 'first',
                    'Post Count': 'sum',
                    'Likes': 'sum',
                    'Comments': 'sum',
                    'Shares per post': 'sum',
                    'Reactions': 'sum'
                }).reset_index()

                aggregated_data['Likes Per Post'] = aggregated_data['Likes'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Comments Per Post'] = aggregated_data['Comments'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Shares Per Post'] = aggregated_data['Shares per post'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Reactions Per Post'] = aggregated_data['Reactions'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Total Interactions'] = aggregated_data['Reactions'] + aggregated_data['Shares per post'] + aggregated_data['Comments']
                aggregated_data['Avg Interactions'] = aggregated_data['Total Interactions'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Total Engagement'] = aggregated_data['Total Interactions'] / aggregated_data['Page followers'].replace(0, 1)
                aggregated_data['Avg Engagement'] = aggregated_data['Total Engagement'] / aggregated_data['Post Count'].replace(0, 1)
                aggregated_data['Shares/1000'] = aggregated_data['Shares per post'] / 1000
                aggregated_data['Reactions/1000'] = aggregated_data['Reactions'] / 1000
                aggregated_data['Date'] = f"{start_date} 12:00 AM - {end_date} 12:00 AM"

                final_aggr_data = aggregated_data.rename(columns={
                    'Post Count': 'Posts',
                    'Likes': 'Total Likes',
                    'Comments': 'Total Comments',
                    'Shares per post': 'Total Shares',
                    'Reactions': 'Total Reactions',
                })
                
                google_api_object.write_to_gsheet(spreadsheet_id=gsheet_name,sheet_name="FB_agg_data",data_df=final_aggr_data)   
                st.write("FACEBOOK AGG DATA SHEET IS CREATED")         
    else:
        st.write("Please Enter a Valid Google Sheet_Id")

def app_third_block():
    import streamlit as st
    import google.auth
    from googleapiclient.discovery import build
    import requests
    import json
    import pandas as pd
    from google.oauth2 import service_account
    import pandas as pd
    import pygsheets
    import gspread
    import numpy as np
    from datetime import datetime,timedelta
    import seaborn as sns
    from oauth2client.service_account import ServiceAccountCredentials
    from googleapiclient.discovery import build
    import streamlit.components.v1 as components
    import tempfile


    # Custom CSS to style the app
    st.markdown("""
        <style>
        .main {
            padding: 20px;
        }
        .reportview-container .main .block-container {
            padding-top: 2rem;
            padding-right: 2rem;
            padding-left: 2rem;
            padding-bottom: 2rem;
        }
        .stButton button {
            background-color: #4CAF50;
            color: white;
            border-radius: 5px;
            padding: 0.5rem 1rem;
            border: none;
            cursor: pointer;
            font-size: 1rem;
        }
        .stButton button:hover {
            background-color: #45a049;
        }
        .stTextInput>div>div>input {
            border: 2px solid #ccc;
            border-radius: 5px;
            padding: 10px;
        }
        .stTextArea textarea {
            border: 2px solid #ccc;
            border-radius: 5px;
            padding: 10px;
        }
        .sidebar .sidebar-content {
            background-color: #f8f9fa;
            padding: 20px;
        }
        .stRadio label {
            font-size: 1rem;
            margin: 0.5rem 0;
        }
        </style>
        """, unsafe_allow_html=True)



    def get_gspread_client():
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        return client

        
    def get_sheet_data(sheet_id):
        client = get_gspread_client()
        sheet = client.open_by_key(sheet_id)
        return sheet
            
    st.title("YOUTUBE SOCIAL MEDIA REQUEST")
    spreadsheet_id = st.text_input("Enter Google Sheet ID")
        
    if spreadsheet_id:
        sheet = get_sheet_data(spreadsheet_id)
        sheet_names = [worksheet.title for worksheet in sheet.worksheets()] 

    if spreadsheet_id:
        sheet_name = st.selectbox("Select Sheet Name", options=sheet_names)
        today_date = datetime.today().date()
        start_date = str(st.date_input("Start Date", value=today_date))
        end_date = str(st.date_input("End Date", value=today_date))

        user_channel_id = st.text_input("Enter YouTube-Channel-ID using { https://commentpicker.com/youtube-channel-id.php }")

        if st.button("Fetch Data"):
            raw_sheet_name = 'Youtube_Channel report'
            agg_sheet_name = 'Youtube_Video wise report'
            class google_api_class:
                
                def __init__(self, credential_file=None):
                    """
                    initializing google api class with parameters from configurations file
                    :return: none
                    """
                    self.credentials = credential_file
                    self.gapi_service = self.initialize_gapi_services()

            
                def initialize_gapi_services(self):
                    """
                    initializing google api services to connect the excel endpoint
                    :return: api instance
                    """
                    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
                    SERVICE_ACCOUNT_FILE = self.credentials
                    gapi_credentials = service_account.Credentials.from_service_account_file(
                        SERVICE_ACCOUNT_FILE, scopes=SCOPES
                    )
                    service = build("sheets", "v4", credentials=gapi_credentials)
                    sheet = service.spreadsheets()
                    return sheet

                
                def append_google_sheets(self, data=None, spreadsheet_id = None,sub_sheet_name=None, sheet_range=None):
                    request = self.initialize_gapi_services().values().append(
                        spreadsheetId=spreadsheet_id,
                        range=sub_sheet_name + "!" + sheet_range,
                        valueInputOption="USER_ENTERED",
                        body={"values": data.values.tolist()},
                    )
                    request.execute()
                    pass
                
            
                def read_sheet(self, sheet_range=None, sheet_name=None, sheet_id=None):
                    """
                    This function is used to get data from a google sheet
                    :param sheet_id: sheet_id of the google spreadsheet
                    :param sheet_range: range from which data is to be read
                    :param sheet_name: name of the sub sheet in the google sheet from where data is to be read
                    :return: list of lists
                    """
                    request = (
                        self.gapi_service.values()
                        .get(spreadsheetId=sheet_id, range=sheet_name + "!" + sheet_range)
                        .execute()
                    )
                    values = request.get("values", [])
                    resp_df = pd.DataFrame(data=values, columns=values[0])
                    resp_df = resp_df.iloc[1:, :]
                    return resp_df


            
                def write_to_gsheet(self, spreadsheet_id=None, sheet_name=None, data_df=None):
                    """
                    this function takes data_df and writes it under spreadsheet_id
                    and sheet_name using your credentials under service_file_path
                    """
                    service_file_path = self.credentials
                    gc = pygsheets.authorize(service_file=service_file_path)
                    sh = gc.open_by_key(spreadsheet_id)
                    # print(sh)
                    try:
                        sh.add_worksheet(sheet_name)
                        # print("Success")
                    except:
                        pass
                    wks_write = sh.worksheet_by_title(sheet_name)
                    wks_write.clear("A1", None, "*")
                    wks_write.set_dataframe(data_df, (1, 1), encoding="utf-8", fit=True)
                    wks_write.frozen_rows = 1

            Google_api_credential_file = dict(st.secrets["gcp_service_account"])
            with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
                temp_file.write(json.dumps(Google_api_credential_file).encode())
                temp_file_path = temp_file.name
            google_api_object = google_api_class(credential_file=temp_file_path)


            def input_df(sheet_name=None, sheet_id=None):
                profile_df = google_api_object.read_sheet(sheet_range='A1:AZ30000', sheet_name=sheet_name, sheet_id=sheet_id)
                profile_df_list = profile_df["Youtube Link"].values.tolist()
                profile_df_list = [link for link in profile_df_list if link]  
                return profile_df_list
        
            def get_channel_ids(urls, api_key):
                channel_id_dict = {}
                for channel_name in urls:
                    service = build('youtube', 'v3', developerKey=api_key)
                    request = service.search().list(
                        type='channel',
                        q=channel_name, 
                        part='id',
                        maxResults=1
                    )
                    response = request.execute()
                    try:
                        channel_id = response['items'][0]['id']['channelId']
                        channel_id_dict[channel_name] = channel_id
                    except Exception as e:
                        # print(f"Unable to get channel id for {channel_name} - {str(e)}")
                        st.write(f"PLEASE ENTER THE CORRECT CHANNEL LINK FOR {channel_name} IN THE SHEET")

                return list(channel_id_dict.values())

            api_key = st.secrets["yut_tokens"]
            urls = input_df(sheet_name,spreadsheet_id)
            # print(urls)
            if user_channel_id:
                channel_ids = [user_channel_id]
            else:
                channel_ids = get_channel_ids(urls, api_key)
                # print(channel_ids)
            youtube = build('youtube', 'v3', developerKey=api_key)

        
            def get_channel_stats(youtube, channel_ids):
                all_data = []
                request = youtube.channels().list(
                            part='snippet,contentDetails,statistics',
                            id=','.join(channel_ids))
                response = request.execute() 
                
                for i in range(len(response['items'])):
                    url = response['items'][i]['snippet']['customUrl'].split('@')
                    data = dict(Channel_name = response['items'][i]['snippet']['title'],
                                Channel_Description = response['items'][i]['snippet']['description'],
                                Channel_link = 'https://www.youtube.com/c/'+url[1],
                                Channel_Published_At = response['items'][i]['snippet']['publishedAt'],
                                Subscribers = response['items'][i]['statistics']['subscriberCount'],
                                Views = response['items'][i]['statistics']['viewCount'],
                                Total_videos = response['items'][i]['statistics']['videoCount'],
                                playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
                    all_data.append(data)
                
                return all_data

        
            def get_video_ids(youtube, playlist_id):
                
                request = youtube.playlistItems().list(
                            part='contentDetails',
                            playlistId = playlist_id,
                            maxResults = 50)
                response = request.execute()
                
                video_ids = []
                
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])
                    
                next_page_token = response.get('nextPageToken')
                more_pages = True
                
                while more_pages:
                    if next_page_token is None:
                        more_pages = False
                    else:
                        request = youtube.playlistItems().list(
                                    part='contentDetails',
                                    playlistId = playlist_id,
                                    maxResults = 50,
                                    pageToken = next_page_token)
                        response = request.execute()
                
                        for i in range(len(response['items'])):
                            video_ids.append(response['items'][i]['contentDetails']['videoId'])
                        
                        next_page_token = response.get('nextPageToken')
                    
                return video_ids

        
            def get_video_details(youtube, video_ids):
                all_video_stats = []
                
                for i in range(0, len(video_ids), 50):
                    request = youtube.videos().list(
                                part='snippet,statistics',
                                id=','.join(video_ids[i:i+50]))
                    response = request.execute()
                    
                    for video in response['items']:
                        try:
                            tags_temp = ','.join(video['snippet']['tags'])
                        except Exception:
                            tags_temp = ''
                        try:
                            lang = video['snippet']['defaultAudioLanguage']
                        except Exception:
                            lang = ''
                        try:
                            commentCount = video['statistics']['commentCount']
                        except Exception:
                            commentCount = ''
                        try:
                            likeCount = video['statistics']['likeCount']
                        except Exception:
                            likeCount = ''
                        try:
                            viewCount = video['statistics']['viewCount']
                        except Exception:
                            viewCount = ''
                        try:
                            liveBroadcastContent = video['snippet']['liveBroadcastContent']
                        except Exception:
                            liveBroadcastContent = ''
                        try:
                            channelTitle = video['snippet']['channelTitle']
                        except Exception:
                            channelTitle = ''
                        try:
                            title = video['snippet']['title']
                        except Exception:
                            title = ''
                        try:
                            description = video['snippet']['description']
                        except Exception:
                            description = ''
                        try:
                            publishedAt = video['snippet']['publishedAt']
                        except Exception:
                            publishedAt = ''
                        try:
                            vid_id = video['id']
                        except Exception:
                            vid_id = ''
                        try:
                            kind = video['kind']
                        except Exception:
                            kind = ''

                        try:
                            hashtag = []
                            textList = description.split()
                            for i in textList:
                                if(i[0]=="#"):
                                    hashtag.append(i)
                            hashtag = ''.join(hashtag)
                        except Exception:
                            hashtag = ''


                        video_stats = dict(Channel = channelTitle,
                                        Title = title,
                                        Description = description,
                                        Published_date = publishedAt,
                                        Video_ID = vid_id,
                                        Video_link = 'https://www.youtube.com/watch?v=' + str(vid_id),
                                        Video_type = kind,
                                        Views = viewCount,
                                        Likes = likeCount,
                                        Comments = commentCount,
                                        Tags = tags_temp,
                                        defaultAudioLanguage = lang,
                                        liveBroadcastContent =liveBroadcastContent,
                                        hashtag = hashtag
                                        )
                        all_video_stats.append(video_stats)
                
                return all_video_stats


            channel_statistics = get_channel_stats(youtube, channel_ids)
            channel_data = pd.DataFrame(channel_statistics)
            channel_data['Channel_Published_At'] = pd.to_datetime(channel_data['Channel_Published_At'], errors='coerce', utc=True)
            channel_data['Channel_Published_At'] = channel_data['Channel_Published_At'].dt.date
            channel_data['Subscribers'] = pd.to_numeric(channel_data['Subscribers'], errors='coerce')
            channel_data['Views'] = pd.to_numeric(channel_data['Views'], errors='coerce')
            channel_data['Total_videos'] = pd.to_numeric(channel_data['Total_videos'], errors='coerce')

            lists = channel_data['Channel_name'].tolist()

            all_playlist_id = []
            for i in channel_data['Channel_name']:
                each_playlist_id = channel_data.loc[channel_data['Channel_name']==i, 'playlist_id'].iloc[0]
                all_playlist_id.append(each_playlist_id)


            all_video_ids = []
            for count,item in enumerate(all_playlist_id):
                video_ids = get_video_ids(youtube, item)
                all_video_ids.append(video_ids)


            all_video_details = []
            for count,item in enumerate(all_video_ids):
                video_details = get_video_details(youtube, item)
                all_video_details.append(video_details)

            all_video_details = [pd.DataFrame(item) if isinstance(item, list) else item for item in all_video_details]

            df_temp = pd.concat(all_video_details, ignore_index=True)
            df_temp['Channel'].unique()
            df_temp['Published_date'] = pd.to_datetime(df_temp['Published_date']).dt.date
            df_temp['Published_date'] = pd.to_datetime(df_temp['Published_date'])
            df_temp = df_temp[(df_temp['Published_date'] >= start_date) & (df_temp['Published_date'] <= end_date)]


            df_temp['Views'] = pd.to_numeric(df_temp['Views'], errors='coerce')
            df_temp['Likes'] = pd.to_numeric(df_temp['Likes'], errors='coerce')
            df_temp['Comments'] = pd.to_numeric(df_temp['Comments'], errors='coerce')

            channel_data['Subscribers'] = pd.to_numeric(channel_data['Subscribers'], errors='coerce')
            channel_data['Views'] = pd.to_numeric(channel_data['Views'], errors='coerce')

            temp_grouped = df_temp.groupby('Channel').agg({
                'Views': 'sum',
                'Likes': 'sum',
                'Comments': 'sum',
                'Video_ID': pd.Series.nunique
            }).reset_index()

            consolidated_data = pd.merge(channel_data, temp_grouped, left_on='Channel_name', right_on='Channel', how='left')

            consolidated_data['Views_x'] = np.where(
                consolidated_data[['Likes', 'Comments', 'Video_ID']].isnull().any(axis=1),
                np.nan,
                consolidated_data['Views_x']
            )

            final_df = consolidated_data[['Channel_name', 'Subscribers', 'Views_y', 'Likes', 'Comments', 'Video_ID']]
            final_df.columns = ['Channel', 'Subscribers', 'Sum of Views', 'Sum of Likes', 'Sum of Comments', 'Unique Video_ID Count']
            # print(final_df)

            def update_sheet(raw_df = None,agg_df = None,google_api_object = None,raw_sheet_name = None,agg_sheet_name = None,spreadsheet_id = None):
                google_api_object.write_to_gsheet(spreadsheet_id=spreadsheet_id, sheet_name=raw_sheet_name, data_df=raw_df)
                google_api_object.write_to_gsheet(spreadsheet_id=spreadsheet_id, sheet_name=agg_sheet_name, data_df=agg_df)
                st.write('Youtube Social Media Report Generated')

            update_sheet(raw_df = final_df,agg_df = df_temp,google_api_object = google_api_object,raw_sheet_name = raw_sheet_name,agg_sheet_name = agg_sheet_name,spreadsheet_id = spreadsheet_id)
            google_api_object.write_to_gsheet(sheet_name="Channel_data",spreadsheet_id=spreadsheet_id,data_df=channel_data)
            st.write('Channel Data Report Generated')

def main():
    if 'selected_function' not in st.session_state:
        st.session_state.selected_function = None

    with st.sidebar:
        option = st.radio(
            "Select which function you want to run:",
            ('INSTAGRAM', 'FACEBOOK','YOUTUBE'),
            on_change=lambda: st.session_state.update({"selected_function": None})
        )

        if st.button("OK"):
            st.session_state.selected_function = option

    if not st.session_state.selected_function:      
        html_content = """
        <div style="
            border-radius: 20px;
            box-shadow: 5px 5px 5px #2691a9;
            overflow: hidden;
        ">
            <img src="https://media.licdn.com/dms/image/C4D0BAQHxlx31iRVpcQ/company-logo_200_200/0/1654155578017?e=1726099200&v=beta&t=41pCBzTxlFnZG43IlklTbQpRnirE8szdB27p8zN2HFg" 
                 style="width: 100px; height: auto;">
        </div>
        """
        components.html(html_content, height=120)      
        st.title("Choose the operation to perform")

    if st.session_state.selected_function == 'INSTAGRAM':
        app_first_block()
    elif st.session_state.selected_function == 'FACEBOOK':
        app_second_block()
    elif st.session_state.selected_function == 'YOUTUBE':
        app_third_block()

if __name__ == "__main__":
    main()
