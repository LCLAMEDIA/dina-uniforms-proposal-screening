from notion_client import Client
import os, sys
from datetime import date, datetime
import json

from Analysis import Analysis


class NotionOperator:
    def __init__(
        self,
        api_key=os.environ["NOTION_KEY"],
        database_id="6ed5d66dff0a411cab7f7caa0c977661",
    ):
        self.client = Client(auth=api_key)
        self.database_id = database_id

    def create_heading_block(self, title, heading_style="heading_2"):
        return {
            "object": "block",
            "type": heading_style,
            heading_style: {
                "rich_text": [{"type": "text", "text": {"content": title}}]
            },
        }

    def create_toggle_block(self, title, children=[]):
        return {
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [{"type": "text", "text": {"content": title}}],
                "children": children,
            },
        }

    def create_bullet_point(self, title, subtext=None, children=[], bold=True, underline=False):
        text = [
            {"type": "text", "text": {"content": title}, "annotations": {"bold": True, "underline": underline}}
        ]
        if subtext is not None:
            text.append(
                {
                    "type": "text",
                    "text": {"content": f" - {subtext}"},
                    "annotations": {"bold": False},
                }
            )
        return {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": text,
                "children": children,
            },
        }
    def create_image_block(self, image_url):
        return {
            "object": "block",
            "type": "image",
            "image": {
                "external":  {
                    "url": image_url
                }
            }
        }

    def create_paragraph_block(self, content, link=None, code=False):
        text_config = {"content": content}
        if link is not None:
            text_config["link"] = {"url": link}

        return {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "text": text_config, "annotations": {"code": code}}
                ]
            },
        }

    def create_properties(self, properties):
        new_properties = {}
        for key, value in properties.items():
            if value.get("type") == "date":
                new_properties[key] = {"type": "date", "start": value["content"]}
        return new_properties

    def format_analysis(self, analysis: Analysis):
        # Headings
        prompt_section = self.create_heading_block(
            f"{analysis.prompt_obj.get('display_name')}"
        )

        # Description
        description = self.create_paragraph_block(
            f"{analysis.prompt_obj.get('description')}", code=True
        )

        # Analysis
        analysis_text = self.create_paragraph_block(
            f"{analysis.response.get('analysis')[0:1900]}"
        )

        # Dot Pointgs
        dot_points = []
        for dot_point in analysis.response.get("dot_point_summary"):
            for key, item in dot_point.items():
                dot_points.append(
                    self.create_bullet_point(
                        key, underline=True, children=[self.create_paragraph_block(item)]
                    )
                )

        return self.create_toggle_block(
            f"{analysis.prompt_obj.get('display_name')}",
            [prompt_section, description, analysis_text] + dot_points,
        )

    def format_timeline(self, timeline: Analysis):
        # Headings
        prompt_section = self.create_heading_block(
            f"{timeline.prompt_obj.get('display_name')}"
        )

        # Description
        description = self.create_paragraph_block(
            f"{timeline.prompt_obj.get('description')}", code=True
        )

        # Timeline Items
        timeline_items = []
        for timeline_item in timeline.response.get("timeline"):
            for key, item in timeline_item.items():
                timeline_items.append(self.create_bullet_point(key, subtext=item))

        return [prompt_section, description] + timeline_items

    def format_cost_value(self, cost_value: Analysis):
        # Headings
        prompt_section = self.create_heading_block(
            f"{cost_value.prompt_obj.get('display_name')}"
        )

        # Description
        description = self.create_paragraph_block(
            f"{cost_value.prompt_obj.get('description')}", code=True
        )

        # cost_value Items
        cost_value_items = []
        for cost_value_item in cost_value.response.get("cost_value"):
            for key, item in cost_value_item.items():
                cost_value_items.append(self.create_bullet_point(key, subtext=item))
        
        return [prompt_section, description] + cost_value_items

    def create_page_from_analysis(
        self, proposal_name: str, analysis_list: list[Analysis]
    ):
        current_date = datetime.now().strftime("%Y-%m-%d")

        id = {
            "Title": {"title": [{"text": {"content": f"[{proposal_name}] Analysis"}}]}
        }
        properties = id | {"Date": {"type": "date", "date": {"start": current_date}}}

        # Create a toggle block for each analysis
        analysis_blocks = []
        dot_point_blocks = []
        for idx, analysis in enumerate(analysis_list):
            if (
                "analysis" in analysis.response
            ):  # TODO - Change this to a class variable and handle in there?
                dot_point_blocks.append(self.format_analysis(analysis))
            elif "timeline" in analysis.response:
                timeline_block = self.format_timeline(analysis)
            elif "cost_value" in analysis.response:
                cost_value_block = self.format_cost_value(analysis)
            
        # Initialise the order of output    
        analysis_blocks += timeline_block
        analysis_blocks += cost_value_block
        for dot_point_block in dot_point_blocks:
            analysis_blocks.append(dot_point_block)

        # Create page children objects
        children = [
            self.create_heading_block(f"[{proposal_name}] Analysis - {current_date}")
        ]
        children += analysis_blocks
        self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties,
            children=children,
        )

    def create_test_page(self):
        id = {"Title": {"title": [{"text": {"content": "Test"}}]}}
        properties = id | {"Date": {"type": "date", "date": {"start": "2023-10-10"}}}
        children = [
            self.create_heading_block("Test Heading 1"),
            self.create_image_block("https://cdn.discordapp.com/attachments/1217668035742011502/1234274572069765211/Dina_Corporate_logo_inline_RGB.jpg"),
            self.create_toggle_block(
                "Test Toggle", [self.create_paragraph_block("Test Paragraph")]
            ),
            self.create_bullet_point(
                "test_bullet_point", [self.create_paragraph_block("test text")]
            ),
        ]
        self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties,
            children=children,
        )
