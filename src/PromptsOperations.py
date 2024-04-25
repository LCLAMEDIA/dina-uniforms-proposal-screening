class PromptsOperations:
    def __init__(self):
        self.all_prompts = [self.timelines_prompt, self.cost_value_prompt, self.eligibility_prompt, self.in_person_requirements_prompt]
        self.prompt_mapping = {
            'timelines_prompt': self.timelines_prompt,
            'cost_value_prompt': self.cost_value_prompt,
            'eligibility_prompt': self.eligibility_prompt,
            'in_person_requirements_prompt': self.in_person_requirements_prompt,
        }

    def get_system_prompt(self):
        return "You are assessing a new tender proposal for our company, we are trying to win the tender to do business with this client. You are trying to identify any aspects of this proposal that we should be aware of/bring attention to. Check carefully for small terms and conditions that may trip us up, summarise requirements that we need to fulfill"
   
    def timelines_prompt(self):
        return {
                "name": "timelines_prompt",
                "display_name": "Proposal Timelines",
                "description": "Analysis specific to project timelines",
                "prompt": """
            Analyse the following extract for a tender proposal. You are looking specifically at key dates to formulate a timeline.
            Return a timeline of key dates and what they involve for this proposal. Including the product volume of the deliverable if it exists.
            The keys should be the dates (either exact date or month), and the values should be the description/requirement. Only include results if they reference a particular date. If there are no dates int this extract, dont include any.
            Your Output should be a valid JSON response of format:
            {
                "timeline": [
                    {'date1': "Description of date1"},
                    {'date2': "Description of date2"}
                ]
            }"""
        }
        
    def cost_value_prompt(self):
        return {
                "name": "cost_value_prompt",
                "display_name": "Cost & Value Analysis",
                "description": "Analysis specific to cost and value of proposal",
                "prompt": """
            Analyse the following extract for a tender proposal. You are looking specifically at the cost and value associated with this proposal. 
            Analyse any dollar values mentioned, as well as volume of product (Only if it mentions the number). Any cost implications to the business. Include a dot point heading as the key, and a short (max 1 sentence) description as the value.
            Your Output should be a valid JSON response of format:
            {
                "cost_value": [
                    {"cost value item 1": "cost description"},
                    {"cost value item 2": "cost_description"}
                ]
            }"""
        }
   
    def eligibility_prompt(self):
        return {
            "name": "eligibility_prompt",
            "display_name": "Proposal Eligibility",
            "description": "Analysis specific to proposal eligibility - Aspects needing consideration before pursuing",
            "prompt": """
        Analyse the following extract for a tender proposal. You are looking specifically at things relating to eligibility for applying for this tender. Identify potential risks and aspects of this tender that relate to eligibility for application and summarise them for me. 
        Try to generate at least 4 dot points for the following output json response
        Your Output should be a valid JSON response of format:
        {
            "analysis": "Your Full Analysis",
            "dot_point_summary": [
                {'your dot point title': "Your analysis/reasoning/reference to proposal for this dot point"},
                {'your second dot point title': "Your analysis/reasoning/reference to proposal for this dot point"}
            ]
        }"""
        }
        
    def in_person_requirements_prompt(self):
        return {
            "name": "in_person_requirements_prompt",
            "display_name": "In Person Requirements",
            "description": "Analysis specific to in-person requirements necessary for this proposal",
            "prompt": """
        Analyse the following extract for a tender proposal. You are looking specifically for things related to in-person requirements for applying for this tender. This can include (but not limited to) staff that have to be physically at a certain area as a part of this tender (e.g 2 Permanent staff needed at a particular city)
        Your output should be a valid JSON response of format:
        {
            "analysis": "Your Full analysis",
            "dot_point_summary": [
                {'your dot point title': "Your analysis/reasoning for this dot point"},
                {'your second dot point title': "Your analysis/reasoning/reference to proposal for this dot point"}
            ]
        }
        """
        }
        
    def combine_dot_point_prompt(self):
        return {
            "name": "combine_dot_point_prompt",
            "prompt": """
        You will be provided a list of dot points and their analysis from an earlier analysis of a tender proposal. These were generated using different chunks of the same proposal, so some of them may have overlapping information/say the same thing.
        I want you to group together the dot points that are more or less the same or similar but otherwise leave the rest as is. I don't want more than 10 dot points if possible, but if there are more than 10 important ones its fine to leave it in.
        If a dot point includes a specific proposal requirement, make sure to leave that in so we can see what theyre referring to.
        Your response should be a valid json of the following format
        {
            "dot_point_summary": [
                {'your dot point title': "The analysis for this dot point"},
                {'your second dot point title': "The analysis for this dot point"}
            ]
        }
        """
        }

    def combine_analysis_prompt(self):
        return {
            "name": "combine_analysis_prompt",
            "prompt": """
        You will be provided a list of analysis' from an earlier analysis of a tender proposal. These were generated using different chunks of the same proposal, so some of them may have overlapping information/say the same thing.
        I want you to form a cohesive analysis utilising these, making sure that we're not duplicating information. Specify important aspects of the proposal to be aware of. Keep it succinct and to 2 to 3 sentences.
        Your response should be a valid json of the following format
        {
            "analysis": "Your combined analysis"
        }
        """
        }
    
    def combine_cost_value_prompt(self):
        return {
                "name": "combine_cost_value_prompt",
                "prompt": """
            You will be provided a list of cost value items generated from a tender proposal. These were generated using different chunks of the same proposal that may have overlapping items.
            I want you to combine any overlapping items to form the one list of cost value items so we don't have any duplicates.
            Keep it to a maximum of 10 items, prioritise dollar values and numerical figures.
            Your reponse should be a valid json of the following format
            {
                "cost_value": [
                    {"cost value item 1": "cost description"},
                    {"cost value item 2": "cost_description"}
                ]
            }"""
        }
    
    def combine_timelines_prompt(self):
        return {
                "name": "combine_timelines_prompt",
                "prompt": """
            You will be provided a list of timeline date items generated from a tender proposal. These were generated using different chunks of the same proposal that may have overlapping items. 
            I want you to combine these fractured lists into a single, cohesive list. Ensure they are in date order. No Overlaps.
            Your Output should be a valid JSON response of format:
            {
                "timeline": [
                    {'date1': "Description of date1"},
                    {'date2': "Description of date2"}
                ]
            }"""
        }
