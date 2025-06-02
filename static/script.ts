/**
 * TypeScript interface for form data structure
 */
interface CopyFormData {
    masterSegmentId: string;
    apiKey: string;
    instance: string;
    // outputMasterSegmentId: string;
    masterSegmentName: string;
    apiKeyOutput: string;
    copyAssets: boolean;
    copyDataAssets: boolean;
}

/**
 * Type-safe element selector
 */
function getElement<T extends HTMLElement>(id: string): T {
    const element = document.getElementById(id);
    if (!element) throw new Error(`Element with id ${id} not found`);
    return element as T;
}

document.addEventListener("DOMContentLoaded", () => {
    // Get DOM elements using type-safe selector
    const elements = {
        form: getElement<HTMLFormElement>("myForm"),
        statusBox: getElement<HTMLDivElement>("status-box"),
        statusTitle: getElement<HTMLSpanElement>("status-title"),
        statusContent: getElement<HTMLDivElement>("status-content"),
        statusSpinner: getElement<HTMLDivElement>("status-spinner"),
    };

    /**
     * Updates the status display with new information
     */
    function updateStatus(
        title: string,
        content?: string,
        type: "progress" | "error" | "success" = "progress"
    ): void {
        elements.statusBox.className = `status-box show status-${type}`;
        elements.statusTitle.textContent = title;

        if (content) {
            elements.statusContent.textContent += `${content}\n`;
            elements.statusContent.scrollTop =
                elements.statusContent.scrollHeight;
        }
        elements.statusSpinner.style.display =
            type === "progress" ? "block" : "none";
    }

    /**
     * Collects form data in a type-safe manner
     */
    function getFormData(): CopyFormData {
        return {
            masterSegmentId:
                getElement<HTMLInputElement>("masterSegmentId").value,
            apiKey: getElement<HTMLInputElement>("apiKey").value,
            instance: getElement<HTMLSelectElement>("instance").value,
            // outputMasterSegmentId: getElement<HTMLInputElement>("outputMasterSegmentId").value,
            masterSegmentName:
                getElement<HTMLInputElement>("masterSegmentName").value,
            apiKeyOutput: getElement<HTMLInputElement>("apiKeyOutput").value,
            copyAssets: getElement<HTMLInputElement>("copyAssets").checked,
            copyDataAssets:
                getElement<HTMLInputElement>("copyDataAssets").checked,
        };
    }

    elements.form.addEventListener("submit", async (event: Event) => {
        event.preventDefault();
        elements.statusContent.textContent = "";
        updateStatus("Starting process...");

        try {
            const formData = getFormData();

            // Create single EventSource connection with JSON POST request
            const response = await fetch("/submit", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(formData),
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const reader = response.body?.getReader();
            const decoder = new TextDecoder();

            while (reader) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value);
                const lines = chunk.split("\n");

                for (const line of lines) {
                    if (line.startsWith("data: ")) {
                        const eventData = line.slice(6);
                        try {
                            const data = JSON.parse(eventData);
                            const type = data.type as
                                | "error"
                                | "success"
                                | "progress";
                            updateStatus(
                                data.type === "error"
                                    ? "Error Occurred"
                                    : data.type === "success"
                                    ? "Success!"
                                    : "Processing...",
                                data.message,
                                type
                            );

                            if (type === "success" || type === "error") {
                                reader.cancel();
                            }
                        } catch (e) {
                            console.error("Error parsing event data:", e);
                            updateStatus(
                                "Error Occurred",
                                "Failed to parse server response",
                                "error"
                            );
                            reader.cancel();
                        }
                    }
                }
            }
        } catch (error) {
            console.error("Error starting process:", error);
            updateStatus(
                "Error Occurred",
                `An error occurred: ${
                    error instanceof Error ? error.message : String(error)
                }`,
                "error"
            );
        }
    });
});
