class PromptsOperations:
    def __init__(self):
        self.all_prompts = [self.timelines_prompt, self.eligibility_prompt, self.cost_value_prompt, self.in_person_requirements_prompt, self.uniform_specification_prompt, self.customer_support_service_prompt, self.long_term_partnership_potential_prompt, self.risk_management_analysis_prompt]
        self.prompt_mapping = {
            'timelines_prompt': self.timelines_prompt,
            'cost_value_prompt': self.cost_value_prompt,
            'eligibility_prompt': self.eligibility_prompt,
            'in_person_requirements_prompt': self.in_person_requirements_prompt,
            'uniform_specification_prompt': self.uniform_specification_prompt,
            'customer_support_service_prompt': self.customer_support_service_prompt,
            'long_term_partnership_proposal_prompt': self.long_term_partnership_proposal_prompt,
            'risk_management_analsis_prompt': self.risk_management_analysis_prompt
        }

    def get_system_prompt(self):
        return "You are assessing a new tender proposal for our uniform supplying company, we are trying to win the tender to do business with this client. You are trying to identify any aspects of this proposal that we should be aware of/bring attention to. Check carefully for small terms and conditions that may trip us up. Use Australian English."
   
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
        Your Output should be a valid JSON response of format:
        {
            "analysis": "Your Full Analysis",
            "dot_point_summary": [
                {'your dot point title': "Your analysis/reasoning/reference to proposal for this dot point"},
                {'your second dot point title': "Your analysis/reasoning/reference to proposal for this dot point"}
            ]
        }"""
        }
    
    
    def uniform_specification_prompt(self):
        return {
            "name": "uniform_specification_prompt",
            "display_name": "Uniform Specification",
            "description": "Analysis specific to uniform supplying requirements",
            "prompt": """
        Analyse the following extract for a tender proposal to supply uniforms/clothing. You are looking specifically for things related to uniform specification and requirements. Things such as: Bespoke vs Buy (Requires custom items? or standard items), Uniform Allocations based off role (how many items are allocated to full-time,part-time, casual etc). If these two examples arent present, mention that theyre not present. Only return answers directly related to uniform specifications and requirements.
        Your output should be a valid JSON response of format:
        {
            "analysis": "Your Full analysis",
            "dot_point_summary": [
                {'specification 1': "Requirements for this specification"},
                {'specification 2': "requirements for this specification"}
            ]
        }
        """
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
        
    def customer_support_service_prompt(self):
        return {
            "name": "customer_suppoert_service_prompt",
            "display_name": "Customer Support",
            "description": "Analysis specific to customer support",
            "prompt": """ 
            Analyze the customer support services outlined in the tender proposal. Focus on the scope and quality of support provided, including available communication channels and response times. Consider how these services align with the expected standards and requirements of the tender.
            Your output should be a valid JSON response of format:
             {
                "analysis": "Your Full Analysis",
                "dot_point_summary": [
                    {'your dot point title': "Your analysis/reasoning/reference to proposal for this dot point"},
                    {'your second dot point title': "Your analysis/reasoning/reference to proposal for this dot point"}
                ]
            }
            """
        }
        
    def long_term_partnership_potential_prompt(self):
        return {
            "name": "long_term_partnership_potential_prompt",
            "display_name": "Long-term Partnership Potential",
            "description": "Analyze the potential for long-term partnerships beyond the scope of the tender",
            "prompt": """ 
             Analyze the tender proposal to identify elements that suggest the potential for a long-term partnership. Consider factors such as the scalability of services, alignment with future goals, and past performance stability. Evaluate the readiness of the proposing party to adapt to future changes and challenges. 
             Your output should be a valid JSON response of format:
             {
                "analysis": "Your Full Analysis",
                "dot_point_summary": [
                    {'your dot point title': "Your analysis/reasoning/reference to proposal for this dot point"},
                    {'your second dot point title': "Your analysis/reasoning/reference to proposal for this dot point"}
                ]
            }
            """
        }
    def risk_management_analysis_prompt(self):
        return {
            "name": "risk_management_analysis",
            "display_name": "Risk Management",
            "description": "Analyze key risks in the tender proposal",
            "prompt": """
            Analyze the tender proposal to pinpoint potential risks that could undermine the project. Focus on identifying major risks and proposing effective mitigation strategies. 
            Your output should be a valid JSON response of format:
             {
                "analysis": "Your Full Analysis",
                "dot_point_summary": [
                    {'your dot point title': "Your analysis/reasoning/reference to proposal for this dot point"},
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
        I want you to group together the dot points that are more or less the same or similar but otherwise leave the rest as is. I don't want more than 6 dot points - make it succinct.
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
        I want you to form a cohesive analysis utilising these, making sure that we're not duplicating information. Specify important aspects of the proposal to be aware of. Keep it short and succinct and to MAX 2 sentences.
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
            I want you to combine any overlapping items to form the one list of cost value items so we don't have any duplicates. Only include items that have a direct cost/figures.
            Keep it to a maximum of 7 items, prioritise dollar values and numerical figures.
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
