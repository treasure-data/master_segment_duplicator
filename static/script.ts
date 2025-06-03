/**
 * TypeScript interface for form data structure
 */
interface CopyFormData {
    masterSegmentId: string;
    apiKey: string;
    instance: string;
    masterSegmentName: string;
    apiKeyOutput: string;
    copyAssets: boolean;
    copyDataAssets: boolean;
}

interface ProgressMessage {
    type: "progress" | "error" | "success";
    message: string;
    operation_id: string;
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
    // Initialize Socket.IO connection
    const socket = (window as any).io();

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

    // Socket.IO event handlers
    socket.on("connect", () => {
        console.log("Connected to server");
    });

    socket.on("disconnect", () => {
        console.log("Disconnected from server");
        updateStatus("Connection lost", "Attempting to reconnect...", "error");
    });

    socket.on("copy_progress", (data: ProgressMessage) => {
        updateStatus(
            data.type === "error"
                ? "Error Occurred"
                : data.type === "success"
                ? "Success!"
                : "Processing...",
            data.message,
            data.type
        );
    });

    /**
     * Collects form data in a type-safe manner
     */
    function getFormData(): CopyFormData {
        return {
            masterSegmentId:
                getElement<HTMLInputElement>("masterSegmentId").value,
            apiKey: getElement<HTMLInputElement>("apiKey").value,
            instance: getElement<HTMLSelectElement>("instance").value,
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
            socket.emit("start_copy", formData);
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
