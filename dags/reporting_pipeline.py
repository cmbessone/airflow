# reporting_pipeline.py

import os
from datetime import datetime

# Importaciones para el modelo local
from langchain_community.chat_models import ChatOllama
from langchain_community.vectorstores import Chroma
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)

# Importaciones para el PDF
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch

# Obtenemos la ruta de la DB y del output de la misma forma que en el indexador
if "AIRFLOW_HOME" in os.environ:
    CHROMA_DIR = os.getenv("CHROMA_DIR", "/opt/airflow/chromadb_data")
    OUTPUT_DIR = "/opt/airflow/output"
else:
    CHROMA_DIR = os.getenv("CHROMA_DIR", "./chromadb_data")
    OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def query_and_summarize(query: str, ticker: str = "Apple") -> dict:
    """
    Performs RAG to retrieve context from ChromaDB and generate a summary with a LOCAL LLM.
    """
    print(f"Iniciando RAG para la consulta: '{query}'")

    # 1. Inicializar el mismo modelo de embeddings que se usó para indexar (¡esto ya es local!)
    embedding_function = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

    # 2. Conectarse a la VectorDB existente
    vector_store = Chroma(
        persist_directory=CHROMA_DIR, embedding_function=embedding_function
    )

    # 3. Crear un "Retriever" para buscar documentos relevantes
    retriever = vector_store.as_retriever(
        search_type="similarity", search_kwargs={"k": 5}
    )

    relevant_docs = retriever.invoke(query)
    context = "\n\n---\n\n".join([doc.page_content for doc in relevant_docs])
    metadata = relevant_docs[0].metadata if relevant_docs else {}

    # 4. Definir el prompt para el LLM
    prompt_template = """
    You are a financial analyst. Answer the user's question based ONLY on the following context.
    Be concise.

    CONTEXT:
    {context}

    QUESTION:
    {question}

    ANSWER:
    """
    prompt = ChatPromptTemplate.from_template(prompt_template)

    # 5. Inicializar el LLM LOCAL con Ollama
    # Asegúrate de que Ollama esté corriendo en otra terminal con `ollama run tinydolphin`
    # NOTA: Si tu Docker en Mac/Windows no puede acceder a localhost, usa 'host.docker.internal'
    # llm = ChatOllama(model="tinydolphin", base_url="http://host.docker.internal:11434")
    llm = ChatOllama(model="tinydolphin", base_url="http://host.docker.internal:11434")

    # 6. Crear y ejecutar la cadena de RAG
    rag_chain = prompt | llm | StrOutputParser()
    print("Generando resumen con el LLM local...")
    summary = rag_chain.invoke({"context": context, "question": query})

    print("Resumen generado con éxito.")

    return {
        "summary": summary,
        "ticker": metadata.get("ticker", ticker),
        "filedAt": metadata.get("filedAt", "N/A"),
    }


def create_summary_pdf(summary_data: dict, query: str) -> str:
    # Esta función no necesita cambios
    summary = summary_data["summary"]
    ticker = summary_data["ticker"]
    filed_at = summary_data["filedAt"]

    file_path = os.path.join(
        OUTPUT_DIR,
        f"{ticker}_10K_Summary_LOCAL_{datetime.now().strftime('%Y%m%d')}.pdf",
    )
    print(f"Creando PDF en: {file_path}")

    doc = SimpleDocTemplate(file_path)
    styles = getSampleStyleSheet()
    story = []

    story.append(
        Paragraph(f"Resumen de Reporte 10-K para {ticker} (Modelo Local)", styles["h1"])
    )
    story.append(Spacer(1, 0.2 * inch))
    story.append(
        Paragraph(
            f"<b>Fecha del Filing de Referencia:</b> {filed_at}", styles["BodyText"]
        )
    )
    story.append(Paragraph(f"<b>Consulta Realizada:</b> {query}", styles["BodyText"]))
    story.append(Spacer(1, 0.4 * inch))
    story.append(Paragraph("--- RESUMEN GENERADO POR IA LOCAL ---", styles["h3"]))
    story.append(Spacer(1, 0.2 * inch))
    formatted_summary = summary.replace("\n", "<br/>")
    story.append(Paragraph(formatted_summary, styles["BodyText"]))

    doc.build(story)
    print("PDF creado con éxito.")
    return file_path
