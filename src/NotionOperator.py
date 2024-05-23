from notion_client import Client
import os, sys
from datetime import date, datetime
import json
import logging
from Analysis import Analysis
from dotenv import load_dotenv

load_dotenv()

class NotionOperator:
    def __init__(
        self,
        api_key=os.environ["NOTION_KEY"],
        database_id="6ed5d66dff0a411cab7f7caa0c977661",
    ):
        self.client = Client(auth=api_key)
        self.database_id = database_id

    
    def create_blank_page(self, title):
        current_date = datetime.now().strftime("%Y-%m-%d")
        id = {
            "Title": {"title": [{"text": {"content": f"[{title}] Proposal Analysis"}}]}
        }
        properties = id | {"Date": {"type": "date", "date": {"start": current_date}}}
        res = self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties
        )
        page_id = res.get('id')
        page_url = res.get('url')
        logging.info(f"Created Report page with ID = {page_id}")
        return page_id, page_url
        
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

    def create_table(self, table_data):
        if table_data and isinstance(table_data[0], dict):
            table_width = len(table_data[0].keys())
        else:
            table_width = 3  # Fallback to a default value if table_data is empty or invalid

        table_block = {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": table_width,
                "has_column_header": True,
                "has_row_header": False,
                "children": []  # Add an empty list for children
            },
        }

        # Add column headers
        if table_data and isinstance(table_data[0], dict):
            column_headers = [[{"text": {"content": key}}] for key in table_data[0].keys()]
            table_block["table"]["children"].append({"table_row": {"cells": column_headers}})

        # Add table rows
        for row in table_data:
            if isinstance(row, dict):
                cells = [[{"text": {"content": str(value)}}] for value in row.values()]
                if len(cells) == table_width:
                    table_block["table"]["children"].append({"table_row": {"cells": cells}})
                else:
                    print(f"Warning: Skipping row with incorrect number of cells: {row}")

        return table_block
    
    def format_table(self, analysis: Analysis):
        # Headings
        prompt_section = self.create_heading_block(
            f"{analysis.prompt_obj.get('display_name')}"
        )

        # Description
        description = self.create_paragraph_block(
            f"{analysis.prompt_obj.get('description')}", code=True
        )

        analysis_text = self.create_paragraph_block(
            f"{analysis.response.get('analysis')[0:1900]}"
        )

        # Table
        table_data = analysis.response.get("table", [])
        table_block = self.create_table(table_data)
        print(f"""
            ..... Initializing table_block
            prompt_section: {prompt_section}
            description: {description}
            analysis_text: {analysis_text}
            table_block: {table_block}
            """)

        return [prompt_section, description, analysis_text] + [table_block]
    
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
        print(f"""
              ..... Initializing analysis_block
              analysis_prompt_section: {prompt_section}
              analysis_description: {description}
              analysis_analysis_text: {analysis_text}
              """)

        # Dot Pointgs
        dot_points = []
        for dot_point in analysis.response.get("dot_point_summary"):
            for key, item in dot_point.items():
                dot_points.append(
                    self.create_bullet_point(
                        key, underline=True, subtext=item
                    )
                )

        return [prompt_section, description, analysis_text] + dot_points

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

    def create_page_from_analysis(self, proposal_name: str, analysis_list: list[Analysis], page_id: str):
        current_date = datetime.now().strftime("%Y-%m-%d")

        analysis_blocks = []
        dot_point_blocks = []
        for idx, analysis in enumerate(analysis_list):
            if "table" in analysis.response:
                table_block = self.format_table(analysis)
                analysis_blocks += table_block
            elif "analysis" in analysis.response:
                dot_point_blocks += self.format_analysis(analysis)
            elif "timeline" in analysis.response:
                timeline_block = self.format_timeline(analysis)
                analysis_blocks += timeline_block
            elif "cost_value" in analysis.response:
                cost_value_block = self.format_cost_value(analysis)
                analysis_blocks += cost_value_block

        # Create page children objects
        children = [
            self.create_heading_block(f"[{proposal_name}] Analysis - {current_date}")
        ]
        children += analysis_blocks
        self.client.blocks.children.append(block_id=page_id, children=children)

    def create_test_page(self):
        id = {"Title": {"title": [{"text": {"content": "Test @liam@lclamedia.com"}}]}}
        properties = id | {"Date": {"type": "date", "date": {"start": "2023-10-10"}}}
        children = [
            self.create_heading_block("Test Heading 1"),
            self.create_image_block("https://cdn.discordapp.com/attachments/1217668035742011502/1234274572069765211/Dina_Corporate_logo_inline_RGB.jpg"),
            self.create_toggle_block(
                "Test Toggle", [self.create_paragraph_block("Test Paragraph")]
            ),
            self.create_bullet_point(
                "test_bullet_point", [self.create_paragraph_block("test text @liam@lclamedia.com")]
            ),
        ]
        self.client.pages.create(
            parent={"database_id": self.database_id},
            properties=properties,
            children=children,
        )
