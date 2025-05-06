/**
 * TypeScript interface for form data structure
 */
interface CopyFormData {
    masterSegmentId: string;
    apiKey: string;
    instance: string;
    outputMasterSegmentId: string;
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
        statusSpinner: getElement<HTMLDivElement>("status-spinner")
    };

    /**
     * Updates the status display with new information
     */
    function updateStatus(title: string, content?: string, type: 'progress' | 'error' | 'success' = 'progress'): void {
        elements.statusBox.className = `status-box show status-${type}`;
        elements.statusTitle.textContent = title;
        
        if (content) {
            elements.statusContent.textContent += `${content}\n`;
            elements.statusContent.scrollTop = elements.statusContent.scrollHeight;
        }
        elements.statusSpinner.style.display = type === 'progress' ? 'block' : 'none';
    }

    /**
     * Collects form data in a type-safe manner
     */
    function getFormData(): CopyFormData {
        return {
            masterSegmentId: getElement<HTMLInputElement>("masterSegmentId").value,
            apiKey: getElement<HTMLInputElement>("apiKey").value,
            instance: getElement<HTMLSelectElement>("instance").value,
            outputMasterSegmentId: getElement<HTMLInputElement>("outputMasterSegmentId").value,
            masterSegmentName: getElement<HTMLInputElement>("masterSegmentName").value,
            apiKeyOutput: getElement<HTMLInputElement>("apiKeyOutput").value,
            copyAssets: getElement<HTMLInputElement>("copyAssets").checked,
            copyDataAssets: getElement<HTMLInputElement>("copyDataAssets").checked
        };
    }

    /**
     * Processes the streaming response data
     */
    async function processStreamResponse(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
        const decoder = new TextDecoder();
        
        while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            
            const updates = decoder.decode(value)
                .split('\n')
                .filter(line => line.trim())
                .forEach(update => {
                    try {
                        const data = JSON.parse(update);
                        const type = data.type as 'error' | 'success' | 'progress';
                        updateStatus(
                            data.type === 'error' ? 'Error Occurred' :
                            data.type === 'success' ? 'Success!' : 'Processing...',
                            data.message,
                            type
                        );
                    } catch (e) {
                        console.error('Error parsing update:', e);
                        updateStatus('Processing...', update, 'progress');
                    }
                });
        }
    }

    elements.form.addEventListener("submit", async (event: Event) => {
        event.preventDefault();
        elements.statusContent.textContent = '';
        updateStatus("Starting process...");

        try {
            const response = await fetch("/submit", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(getFormData())
            });

            const reader = response.body?.getReader();
            if (!reader) throw new Error("Failed to get response reader");
            
            await processStreamResponse(reader);
        } catch (error) {
            console.error("Error submitting form:", error);
            updateStatus(
                'Error Occurred',
                `An error occurred: ${error instanceof Error ? error.message : String(error)}`,
                'error'
            );
        }
    });
});
