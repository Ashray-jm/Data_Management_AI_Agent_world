from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from crewai.agents.agent_builder.base_agent import BaseAgent
from typing import List
from dotenv import load_dotenv
from crewai_tools import FileWriterTool
from crewai_tools import FileReadTool
from crewai.knowledge.source.text_file_knowledge_source import TextFileKnowledgeSource

load_dotenv()
# If you want to run a snippet of code before or after the crew starts,
# you can use the @before_kickoff and @after_kickoff decorators
# https://docs.crewai.com/concepts/crews#example-crew-class-with-decorators
file_writer = FileWriterTool()
file_reader = FileReadTool()

text_source = TextFileKnowledgeSource(
    file_paths=["user_preference.txt"]
)



@CrewBase
class DataManagementCrew():
    """DataManagementCrew crew"""

    agents: List[BaseAgent]
    tasks: List[Task]

    # Learn more about YAML configuration files here:
    # Agents: https://docs.crewai.com/concepts/agents#yaml-configuration-recommended
    # Tasks: https://docs.crewai.com/concepts/tasks#yaml-configuration-recommended
    
    # If you would like to add tools to your agents, you can learn more about it here:
    # https://docs.crewai.com/concepts/agents#agent-tools
    @agent
    def Analyst(self) -> Agent:
        return Agent(
            config=self.agents_config['Analyst'], # type: ignore[index]
            verbose=True,
            tools=[file_writer]
        )

    @agent
    def ETLDesigner(self) -> Agent:
        return Agent(
            config=self.agents_config['ETLDesigner'],  # type: ignore[index]
            verbose=True,
            tools=[file_writer, file_reader]
        )

    @agent
    def knowledge_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['knowledge_agent'],
            verbose=True,
            tools=[file_reader],
            knowledge_sources=[text_source]
        )


    @agent
    def DataEngineer(self) -> Agent:
        return Agent(
            config=self.agents_config['DataEngineer'],  # type: ignore[index]
            verbose=True,
            tools=[file_writer]
        )

    # To learn more about structured task outputs,
    # task dependencies, and task callbacks, check out the documentation:
    # https://docs.crewai.com/concepts/tasks#overview-of-a-task
    @task
    def analysis_task(self) -> Task:
        return Task(
            config=self.tasks_config['analysis_task'],  # type: ignore[index]
        )

    @task
    def etl_design_task(self) -> Task:
        return Task(
            config=self.tasks_config['etl_design_task'],  # type: ignore[index]
        )

    @task
    def knowledge_fetch_task(self) -> Task:
        return Task(
            # type: ignore[index]
            config=self.tasks_config['knowledge_fetch_task'],
        )


    @task
    def dag_build_task(self) -> Task:
        return Task(
            config=self.tasks_config['dag_build_task'],  # type: ignore[index]
        )

    @crew
    def crew(self) -> Crew:
        """Creates the DataManagementCrew crew"""
        # To learn how to add knowledge sources to your crew, check out the documentation:
        # https://docs.crewai.com/concepts/knowledge#what-is-knowledge

        return Crew(
            agents=self.agents, # Automatically created by the @agent decorator
            tasks=self.tasks, # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
            # process=Process.hierarchical, # In case you wanna use that instead https://docs.crewai.com/how-to/Hierarchical/
        )
