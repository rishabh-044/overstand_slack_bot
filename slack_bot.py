import os
import time
from datetime import datetime, timedelta
import requests
from typing import Dict, Any, List
import pandas as pd
from get_thread_insights import get_ontology_object, get_recent_data, create_attribute_table
import dotenv

dotenv.load_dotenv()

class SlackBot:
    def __init__(self, slack_token: str, channel_id: str):
        """
        Initialize the Slack bot.
        
        Args:
            slack_token: Slack API token (Bot User OAuth Token)
            channel_id: ID of the channel to post messages to
        """
        self.slack_token = slack_token
        self.channel_id = channel_id
        self.base_url = "https://slack.com/api"
        self.headers = {
            "Authorization": f"Bearer {slack_token}",
            "Content-Type": "application/json"
        }
    
    def post_message(self, text: str, blocks: List[Dict[str, Any]] = None) -> bool:
        """
        Post a message to the specified Slack channel.
        
        Args:
            text: The message text (fallback text if blocks fail to render)
            blocks: Optional message blocks for rich formatting
            
        Returns:
            bool: True if message was posted successfully
        """
        url = f"{self.base_url}/chat.postMessage"
        payload = {
            "channel": self.channel_id,
            "text": text  # This is the fallback text
        }
        
        if blocks:
            payload["blocks"] = blocks
            
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            response_data = response.json()
            if not response_data.get("ok"):
                print(f"Slack API error: {response_data.get('error', 'Unknown error')}")
                print(f"Response data: {response_data}")
                return False
                
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Error posting to Slack: {str(e)}")
            return False
    
    def format_insight_message(self, insight: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format a thread insight into a Slack message block.
        
        Args:
            insight: The thread insight data
            
        Returns:
            Dict containing formatted message blocks
        """
        # Initialize blocks list
        blocks = []
        
        # Extract fields from the insight
        title = insight.get("deIdentifiedInsightSummary", "Untitled Insight")
        insight_evidence = insight.get("insightEvidence", "No evidence available")
        sender_role = insight.get("senderRole", "Unknown role")
        org_domain = insight.get("organizationDomain", "Unknown domain")
        insight_type = insight.get("insightType", "Unknown type")
        deidentified_summary = insight.get("deIdentifiedInsightSummary", "No de-identified summary available")
        
        # Truncate title if too long (max 150 chars for header)
        if len(title) > 150:
            title = title[:147] + "..."
        
        # Add header block with the title
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": title,
                "emoji": True
            }
        })
        
        # Add de-identified summary block
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*De-identified Summary:*\n{deidentified_summary}"
            }
        })
        
        # Add insight evidence block
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Evidence:*\n{insight_evidence}"
            }
        })
        
        # Add metadata block
        metadata_text = f"*Type:* {insight_type}\n*Sender Role:* {sender_role}\n*Organization:* {org_domain}"
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": metadata_text
            }
        })
        
        return blocks
    
    def get_and_post_recent_insights(
        self,
        foundry_url: str,
        ontology_rid: str,
        bearer_token: str,
        object_type: str = "ThreadInsight",
        primary_key: str = "internalInsightId",
        hours: int = 24
    ) -> None:
        """
        Fetch recent thread insights and post them to Slack.
        
        Args:
            foundry_url: Foundry instance URL
            ontology_rid: Ontology RID
            bearer_token: Foundry bearer token
            object_type: Type of object to fetch
            primary_key: Primary key for the object
            hours: Number of hours to look back for recent insights (default 24 for daily)
        """
        try:
            # Get all insights
            insights = get_ontology_object(
                base_url=foundry_url,
                ontology_rid=ontology_rid,
                object_name=object_type,
                primary_key=primary_key,
                bearer_token=bearer_token
            )
            
            if not insights:
                self.post_message("No new thread insights found in the last 24 hours.")
                return
            
            # Convert to DataFrame
            df = create_attribute_table(insights)
            
            # Calculate timestamp for last 24 hours
            last_day = (datetime.utcnow() - timedelta(hours=hours)).isoformat() + "Z"
            
            # Get recent insights
            recent_insights = get_recent_data(df, "timestamp", last_day)
            
            if recent_insights.empty:
                self.post_message("No new thread insights found in the last 24 hours.")
                return
            
            # Post a summary message first
            summary_text = f"*Daily Thread Insights Update*\nFound {len(recent_insights)} new insights in the last 24 hours."
            self.post_message(summary_text)
            
            # Post each insight
            for _, insight in recent_insights.iterrows():
                insight_dict = insight.to_dict()
                blocks = self.format_insight_message(insight_dict)
                
                # Create a fallback text for the insight
                fallback_text = f"New Thread Insight: {insight_dict.get('deIdentifiedInsightSummary', 'Untitled Insight')}"
                
                success = self.post_message(fallback_text, blocks)
                if not success:
                    print(f"Failed to post insight: {insight_dict.get('internalInsightId', 'Unknown ID')}")
                time.sleep(1)  # Avoid rate limiting
            
        except Exception as e:
            error_message = f"Error fetching and posting insights: {str(e)}"
            self.post_message(error_message)
            print(error_message)

def run_daily_bot(
    slack_token: str,
    channel_id: str,
    foundry_url: str,
    ontology_rid: str,
    bearer_token: str,
    interval_hours: int = 24
) -> None:
    """
    Run the Slack bot to post insights daily.
    
    Args:
        slack_token: Slack API token
        channel_id: Slack channel ID
        foundry_url: Foundry instance URL
        ontology_rid: Ontology RID
        bearer_token: Foundry bearer token
        interval_hours: Interval in hours between posts (default 24 for daily)
    """
    bot = SlackBot(slack_token, channel_id)
    
    while True:
        try:
            print(f"Fetching insights at {datetime.utcnow().isoformat()}")
            bot.get_and_post_recent_insights(
                foundry_url=foundry_url,
                ontology_rid=ontology_rid,
                bearer_token=bearer_token
            )
            
            # Wait for the next interval
            time.sleep(interval_hours * 3600)
            
        except Exception as e:
            print(f"Error in daily bot: {str(e)}")
            time.sleep(300)  # Wait 5 minutes before retrying

if __name__ == "__main__":
    # Load configuration from environment variables
    SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
    FOUNDRY_URL = os.getenv("FOUNDRY_URL")
    ONTOLOGY_RID = os.getenv("ONTOLOGY_RID")
    BEARER_TOKEN = os.getenv("FOUNDRY_BEARER_TOKEN")
    
    if not all([SLACK_TOKEN, CHANNEL_ID, FOUNDRY_URL, ONTOLOGY_RID, BEARER_TOKEN]):
        print("Error: Missing required environment variables")
        print("Please set the following environment variables:")
        print("- SLACK_BOT_TOKEN: Your Slack bot's OAuth token")
        print("- SLACK_CHANNEL_ID: The ID of the channel to post to")
        print("- FOUNDRY_URL: Your Foundry instance URL")
        print("- ONTOLOGY_RID: The RID of your ontology")
        print("- FOUNDRY_BEARER_TOKEN: Your Foundry bearer token")
        exit(1)
    
    # Start the daily bot
    run_daily_bot(
        slack_token=SLACK_TOKEN,
        channel_id=CHANNEL_ID,
        foundry_url=FOUNDRY_URL,
        ontology_rid=ONTOLOGY_RID,
        bearer_token=BEARER_TOKEN
    ) 