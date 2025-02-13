import streamlit as st
import pandas as pd
import numpy as np
import textblob as TextBlob
import re
from datetime import datetime, timedelta
import altair as alt
pd.options.mode.chained_assignment = None  # default='warn'
from wordcloud import WordCloud
from nltk.corpus import stopwords
import nltk
import matplotlib.pyplot as plt


#Clean Data

# data = pd.read_csv('/Users/mac/Desktop/projects/datasets/whatsapp_analytics/cleaned_data.csv')

 # remove new line and trailing spaces.
def strip_leading_and_newline(text):
    stripped_text =  re.sub(r'^\s+|\s+$', '', text)
    stripped_text = stripped_text.strip()
    return stripped_text

# remove rows without valid date 

def is_valid(date):
    pattern =  r"^(?:[0-9]{2}/[0-9]{2}/[0-9]{4})$"
    valid_date = re.match(pattern, date)
    return valid_date.group() if valid_date else None


# remove rows without valid time
time_pattern = r"^\s*(([0-1]?[0-9]|2[0-3]):([0-5]?[0-9])\s*(?:AM|PM)?|[0-9]:[0-5][0-9])\s*$"

def is_valid_time(time):
  match = re.match(time_pattern, time)
  return match.group(1).strip() if match else None

# function for new_members_dataframe
def get_new_members(message):
    pattern = r'[A-Za-z\s\W]+ added\s?~?\s?[A-Za-z\s\W]+'
    if re.match(pattern, message):
        return message
    return None

# function for exited_members dataframe
def get_exited_members(exited_members, message, member):
    exited_members['extracted_name'] = exited_members[message].str.extract(r'(\w+)\s+left')
    exited_members['left_group'] = exited_members['extracted_name'].str.strip().isin(exited_members[member].str.strip())
    exited_members = exited_members[exited_members['left_group']]
    exited_members.reset_index(drop=True, inplace=True)
    return exited_members


def get_new_and_exited_members_count(new_members_df, date_column, date_range_option):
    filtered_new_members = None
    # Ensure the date column is in datetime format
    new_members_df[date_column] = pd.to_datetime(new_members_df[date_column])
    current_date = datetime.today()

    # Define date ranges based on the selected option
    if date_range_option == "Last 3 days":
        start_date = current_date - timedelta(days=3)
        filtered_new_members = new_members_df[new_members_df[date_column] >= start_date]

    elif date_range_option == "Last Week":
        start_date = current_date - timedelta(days=7)
        filtered_new_members = new_members_df[new_members_df[date_column] >= start_date]

    elif date_range_option == "Last Month":
        start_date = current_date - timedelta(days=30)
        filtered_new_members = new_members_df[new_members_df[date_column] >= start_date]

     # "Anytime" (Return count for the entire dataset)
    return filtered_new_members['member'].nunique() if filtered_new_members is not None else  new_members_df['member'].nunique()


def anonymize_members(df, member_col='member', prefix='Member'):

  member_map = {}
  unique_id = 1

  # Create a dictionary mapping unique IDs to original members
  for member in df[member_col].unique():

    member_map[member] = f'{prefix}{unique_id}'
    unique_id += 1

  # Replace member values with anonymized IDs
  df[member_col] = df[member_col].apply(lambda x: member_map[x])
  return df

# Generate Popular Words
def generate_wordcloud(data, text_column):
    # Download NLTK stopwords if not already downloaded
    nltk.download('stopwords', quiet=True)
    stop_words = set(stopwords.words('english'))
    custom_stop_words = {'media', 'omitted'}
    stop_words.update(custom_stop_words)
    # Combine all messages into a single string
    text = ' '.join(message for message in data[text_column].dropna())
    # Generate the word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white', 
                          stopwords=stop_words, collocations=False).generate(text)
    return wordcloud

# Function to calculate percentage changes for KPIs
def calculate_percentage_change(current, previous):
    if previous == 0:
        return "0.00%"  # Avoid division by zero
    return ((current - previous) / previous) * 100


# Function to get previous dates for KPI 
def get_previous_dates(cleaned_data, date, date_range_option):
  current_date = datetime.today()
  prev_data = None
  if date_range_option == 'Last 3 days':
    start_date = current_date - pd.DateOffset(days=3)
    prev_three_days = start_date - pd.DateOffset(days=6)
    prev_end_date = start_date - pd.DateOffset(days=1)
    prev_data = cleaned_data[(cleaned_data[date] >= prev_three_days) & (cleaned_data[date] <= prev_end_date)]

  elif date_range_option == 'Last Week':
    start_date = current_date - pd.DateOffset(days=7)
    prev_seven_days = start_date - pd.DateOffset(days=14)
    prev_end_date = start_date - pd.DateOffset(days=1)
    prev_data = cleaned_data[(cleaned_data[date] >= prev_seven_days) & (cleaned_data[date] <= prev_end_date)]

  elif date_range_option == 'Last Month':
    start_date = current_date - pd.DateOffset(days=30)
    prev_thirty_days = start_date - pd.DateOffset(days=60)
    prev_end_date = start_date - pd.DateOffset(days=1)
    prev_data = cleaned_data[(cleaned_data[date] >= prev_thirty_days) & (cleaned_data[date] <= prev_end_date)]
  
  return prev_data if prev_data is not None else cleaned_data


# Date Range Filtering 
def filtered_data_by_date(cleaned_data, date,date_range_option):
  cleaned_data[date] = pd.to_datetime(cleaned_data[date])
  current_date = datetime.today()
  filtered_data = cleaned_data
  # Determine the date range based on user selection
  if date_range_option == "Last 3 days":
      start_date = current_date - timedelta(days=3)
      filtered_data= cleaned_data[cleaned_data[date] >= start_date]
  elif date_range_option == "Last Week":
      start_date = current_date - timedelta(days=7)
      filtered_data= cleaned_data[cleaned_data[date] >= start_date]
  elif date_range_option == "Last Month":
      start_date = current_date - timedelta(days=30)
      filtered_data= cleaned_data[cleaned_data[date] >= start_date]
  return filtered_data



def clean_data(uploaded_data): 
# create list of list strucures that removes frst two meesgaes 
    data = uploaded_data[2:]
    cleaned_data = []
     # Detect chat format based on the first valid message
        # Detect chat format based on first valid message
    first_message = data[0] if data else ""
    is_ios = first_message.startswith("[") and "]" in first_message
    is_android = not is_ios and re.match(r"\d{2}/\d{2}/\d{4}, \d{1,2}:\d{2}(?:\s?[apAP][mM])? -", first_message)

    for line in data:
        try:
            if is_ios:
                # iOS Format: [24/02/2023, 10:38:32] User: Message
                datetime_str = line.split("]")[0][1:]  # Removes the opening '['
                if ", " in datetime_str:
                    parts = datetime_str.split(", ")
                    if len(parts) >= 2:
                        date_part = parts[0]
                        time_part = parts[1]
                    else:
                        print("Skipping line due to unexpected format:", line)
                        continue

                # ✅ Extract iOS member & message
                line_remainder = line[len(datetime_str) + 2:]
                member = line_remainder.split(":")[0].strip()
                if member.startswith("~"):
                    member = member[1:].strip()

                messages = line_remainder[len(member) + 1:].strip()
                colon_index = messages.find(":")
                if colon_index >= -1:
                    message = messages[colon_index + 1:].strip()
                cleaned_data.append([date_part, time_part, member, message])
            elif is_android:
                # Android Format: 24/02/2023, 10:38 - User: Message
                match = re.match(r"(\d{2}/\d{2}/\d{4}), (\d{1,2}:\d{2}(?:\s?[apAP][mM])?) - (.*?): (.*)", line)
            else:
                continue  # Skip lines that don't match the format

            if match:
                date_part, time_part, member, message = match.groups()
                # Remove leading "~" in iOS names
                member = member.strip("~").strip()
                # Filter out system messages & empty messages
                if not message.strip() or "Waiting for this message" in message or "image omitted" in message:
                    continue
                cleaned_data.append([date_part, time_part, member, message])
        except Exception as e:
            print(f"Skipping line due to error: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(cleaned_data, columns=["date", "time", "member", "message"])
    df["date"] = df["date"].apply(is_valid)  # Validate before conversion
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce")

    # ✅ Convert time column correctly:
    if is_ios:
        df["time"] = pd.to_datetime(df['time'].str.strip(), format='%H:%M:%S', errors='coerce').dt.time
    elif is_android:
        df["time"] = pd.to_datetime(df["time"], errors="coerce", infer_datetime_format=True).dt.time

    # df['time'] =  pd.to_datetime(df['time'].str.strip(), format='%H:%M:%S', errors='coerce').dt.time
    # df['time'] = pd.to_datetime(df['time'], format='%H:%M', errors='coerce').dt.time

    # ✅ Extract hour for heatmap
    # df["hour"] = df["time"].apply(lambda x: x.hour if pd.notnull(x) else None)
    df['hour'] = pd.to_datetime(df['time'], format='%H:%M:%S').dt.hour

    # ✅ Extract day of the week and month for analytics
    df["dayofweek"] = df["date"].dt.day_name()
    df["month"] = df["date"].dt.month_name()

    # ✅ Store new and exited members in session state
    new_members = df[df["message"].str.contains("added|invited|joined", case=False, na=False)]
    exited_members = df[df["message"].str.contains("left|removed", case=False, na=False)]

    st.session_state["new_members_count"] = new_members
    st.session_state["dropped_member_count"] = exited_members

    # ✅ Remove system messages (group changes, admin messages)
    system_keywords = ["added", "removed", "left", "changed", "created", "pinned", "admin", "group has over", "image omitted", 'Waiting for this message. This may take a while.', "sticker"]
    df = df[~df["message"].str.contains("|".join(system_keywords), case=False, na=False)]

    # ✅ Drop NaN values after removing system messages
    df.dropna(subset=["message"], inplace=True)
    df = anonymize_members(df, 'member', 'Member')
    return df


    


# Application Front End

#  Enter name of the community 
def main():
    if "file_uploaded" not in st.session_state:
      st.session_state.file_uploaded = False

    page = st.sidebar.radio("Choose a page", ['Home', 'Dashboard'])
    # comm_name = " "
    # number_users = 0
    if page == 'Home':
      st.title('WhatsApp Analytics Dashboard')

      # Community Name (Always Uppercase)
      comm_name = st.text_input('Enter the name of the community: ').strip().upper()
      st.session_state['comm_name'] = comm_name

      # Number of Community Members (Ensure a valid input)
      number_users = st.number_input('Enter the number of community members:', min_value=1, step=1)
      st.session_state['number_users'] = int(number_users)

      # 📂 File Upload Section
      st.write("Upload the WhatsApp data for cleaning (txt or csv format):")
      uploaded_file = st.file_uploader("Choose a file", type=["txt", "csv"])

      if uploaded_file is not None:
          try:
              # ✅ Process .txt files (WhatsApp chat exports)
              if uploaded_file.type == "text/plain":
                  data = uploaded_file.read().decode("utf-8")
                  
                  # Process the text data
                  cleaned_data = clean_data(data.splitlines())

              # ✅ Process .csv files
              elif uploaded_file.type == "text/csv":
                  cleaned_data = pd.read_csv(uploaded_file)
                  st.success("CSV file uploaded successfully!")

                  # ✅ Store cleaned data in session state for use in the Dashboard
              st.session_state['cleaned_data'] = cleaned_data

              st.session_state.file_uploaded = True
              if st.session_state.cleaned_data is not None:
                st.success("File uploaded successfully! Data Processing Complete.")
              else:
                st.success("Error In Processing File")
              

          except Exception as e:
              st.error(f"Error processing the file: {e}")

      else:
          st.warning("⚠ Please upload a WhatsApp chat file to proceed.")
      
    elif page == 'Dashboard':
      comm_name = st.session_state['comm_name']
      st.markdown(f"<h1 style='text-align: center; color: white;'>{st.session_state['comm_name'].upper()}</h1>", unsafe_allow_html=True)  
      st.markdown('####')


      # date 
      if 'cleaned_data' in st.session_state:
        cleaned_data = st.session_state['cleaned_data']
        cleaned_data['date'] = pd.to_datetime(cleaned_data['date'])
        
         
      left_column, right_column = st.columns(2)

      with left_column:
        st.subheader('Number of community members: ')
        if st.session_state.number_users is not None:
          st.markdown(f"<h3 style='text-align: left; font-weight: bold;'>{st.session_state.number_users}</h3>", unsafe_allow_html=True)
        else:
          st.write('Please go back to home page to enter the number of community members')
      with right_column:
        st.subheader('Date')
        date_range_option = st.selectbox("Select Date Range", options=["Anytime","Last 3 days", "Last Week", "Last Month"])
        updated_data = filtered_data_by_date(cleaned_data,"date", date_range_option)
        # st.write(cleaned_data)
        previous_data = get_previous_dates(cleaned_data, 'date', date_range_option)
        st.markdown('####')

      # KPIS
      #Current Data

      number_of_new_members = st.session_state['new_members_count'] 
      number_of_new_members['valid_message'] = number_of_new_members['message'].apply(get_new_members)
      number_of_new_members = number_of_new_members[number_of_new_members['valid_message'].notna()]
      # updated_new_members = number_of_new_members[['date', 'valid_message', 'member']]
      number_of_exited_members = st.session_state['dropped_member_count']
      # left_members = get_exited_members(number_of_exited_members, 'message', 'member')
      
      current_active_members = get_new_and_exited_members_count(updated_data, 'date', date_range_option)
      current_new_members = get_new_and_exited_members_count(number_of_new_members, 'date', date_range_option)
      current_exited_members = get_new_and_exited_members_count(number_of_exited_members, 'date', date_range_option)

      # Previous Date
      previous_members = get_previous_dates(number_of_new_members, 'date', date_range_option)
      previous_new_members = int(previous_members['member'].shape[0])
      previous_exited_members = get_new_and_exited_members_count(number_of_exited_members, 'date', date_range_option)
      previous_active_members = int(previous_data['member'].nunique())

      # KPI Percentages 

      percent_change_new_members = calculate_percentage_change(current_new_members, previous_new_members)
      percent_change_exited_members = calculate_percentage_change(current_exited_members, previous_exited_members)
      percent_change_active_members = calculate_percentage_change(current_active_members, previous_active_members)
      # percent_anytime_new_members = ((filtered_new_members + filtered_exited_members) / number_users) * 100


      cols = st.columns([.333, .333, .333])
      with cols[0]:
        st.metric('New Members',  value=current_new_members, delta= f"{float(percent_change_new_members):.2f}%" if isinstance(percent_change_new_members, (int, float)) else "0.00%")
        
      with cols[1]:
        st.metric('Exited Members', value=current_exited_members, delta=f"{float(percent_change_exited_members):.2f}%"if isinstance(percent_change_exited_members, (int, float)) else "0.00%")

      with cols[2]:
        st.metric("Active Members", value=current_active_members, delta=f"{float(percent_change_active_members):.2f}%" if isinstance(percent_change_active_members, (int, float)) else "0.00%")
        st.markdown('####')
      

      cols_1 = st.columns([.666, .666])

      with cols_1[0]:
        st.subheader('Total Messages By Members')
        # Aggregate and display messages per user
        messages_per_user = updated_data.groupby('member').size().reset_index(name='Number of Messages')
        messages_per_user =messages_per_user.sort_values(by='Number of Messages', ascending=False).head(10)
        # Plot the bar chart
        bar_chart = alt.Chart(messages_per_user).mark_bar().encode(
          x=alt.X('member', sort='-x', title='Member'),
          y=alt.Y('Number of Messages', title='Number of Messages'),
          color=alt.Color('member', scale=alt.Scale(scheme='greens'), legend=None)
          ).properties(
            width=600, height=400, title="Total Messages by Members")
        st.altair_chart(bar_chart, use_container_width=True)
        # st.write(messages_per_user.head())

        st.markdown('####')


      with cols_1[1]:
        st.subheader('Total Messages By Month')
        # Aggregate and display messages per date
        # Create ordered categorical type
      
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        updated_data['month'] = pd.Categorical(updated_data['month'], categories=month_order, ordered=True)
        messages_per_month =  updated_data.groupby('month').size().reset_index(name='Number of Messages')
        messages_per_month = messages_per_month.sort_values(by='month')
        # Plot line chart 
        # Create an Altair line chart with explicit month ordering
        line_chart = alt.Chart(messages_per_month).mark_line(color='green').encode(
          x=alt.X('month', sort=month_order, title='Month'),
          y=alt.Y('Number of Messages', title='Number of Messages')).properties(title='Messages per Month')

        # Render the chart in Streamlit
        st.altair_chart(line_chart, use_container_width=True)
        st.markdown('####')

      
      st.subheader('Active Members By Day')
      # Aggregate member counts by day of week and hour
      day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
      updated_data['dayofweek'] = pd.Categorical(updated_data['dayofweek'], categories=day_order, ordered=True)
      active_members_per_day = (updated_data.groupby('dayofweek')['member'].nunique().reset_index(name='Number of Active Members'))
      # Create an Altair bar chart with the specified order
      bar_chart = alt.Chart(active_members_per_day).mark_bar().encode(x=alt.X('dayofweek', sort=day_order, title='Day of the Week'),y=alt.Y('Number of Active Members', title='Number of Active Members'), color= alt.Color('Number of Active Members', scale=alt.Scale(scheme='greens'), legend=None)).properties(title='Active Members by Day of the Week')
      # Display the chart in Streamlit
      st.altair_chart(bar_chart, use_container_width=True)

      
      st.subheader('Engagement Rate By Day & Time')
      # Aggregate message counts by day of week and hour
      heatmap_data = updated_data.groupby(['dayofweek', 'hour']).size().reset_index(name='Number of Messages')
      # Order days of the week for the y-axis
      day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
      heatmap_data['dayofweek'] = pd.Categorical(heatmap_data['dayofweek'], categories=day_order, ordered=True)

        # Create the heatmap using Altair
      heatmap = alt.Chart(heatmap_data).mark_rect().encode(
          x=alt.X('hour:O', title='Hour of Day'),
          y=alt.Y('dayofweek:O', title='Day of Week', sort=day_order),
          color=alt.Color('Number of Messages:Q', scale=alt.Scale(scheme='greens'), title='Number of Messages'),
          tooltip=['dayofweek', 'hour', 'Number of Messages']  # Show details on hover
      ).properties(
          width=600,
          height=400,
          title="Messages by Day and Hour"
      )
      # Display heatmap in Streamlit
      st.altair_chart(heatmap, use_container_width=True)

      st.subheader("Popular Words")
      # Visualize Popular words
      words = generate_wordcloud(updated_data, 'message')
      # Display wordcloud in Streamlit
      plt.figure(figsize=(10, 5))
      plt.imshow(words, interpolation='bilinear')
      plt.axis('off')
      st.pyplot(plt)

  

if __name__ == "__main__":
  main()































# data = pd.read_csv('/Users/mac/Desktop/projects/datasets/whatsapp_analytics/cleaned_data.csv')

