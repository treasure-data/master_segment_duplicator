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

interface ValidationMessage {
    field: string;
    message: string;
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
    // Initialize Socket.IO connection with production settings
    const socket = window.io({
        transports: ["websocket", "polling"], // Prefer WebSocket
        path: "/socket.io", // Default Socket.IO path
        reconnection: true, // Enable reconnection
        reconnectionAttempts: 5, // Limit reconnection attempts
        reconnectionDelay: 3000, // Start with 3 sec delay
        reconnectionDelayMax: 15000, // Maximum delay between reconnections
        timeout: 60000, // Increased connection timeout
    });

    // Get DOM elements using type-safe selector
    const elements = {
        form: getElement<HTMLFormElement>("myForm"),
        statusBox: getElement<HTMLDivElement>("status-box"),
        statusTitle: getElement<HTMLSpanElement>("status-title"),
        statusContent: getElement<HTMLDivElement>("status-content"),
        statusSpinner: getElement<HTMLDivElement>("status-spinner"),
        submitButton: getElement<HTMLButtonElement>("submitButton"),
        inputFields: {
            masterSegmentId: getElement<HTMLInputElement>("masterSegmentId"),
            apiKey: getElement<HTMLInputElement>("apiKey"),
            instance: getElement<HTMLSelectElement>("instance"),
            masterSegmentName:
                getElement<HTMLInputElement>("masterSegmentName"),
            apiKeyOutput: getElement<HTMLInputElement>("apiKeyOutput"),
            copyAssets: getElement<HTMLInputElement>("copyAssets"),
            copyDataAssets: getElement<HTMLInputElement>("copyDataAssets"),
        },
    };

    // Create validation message containers for each input
    Object.entries(elements.inputFields).forEach(([key, input]) => {
        if (key !== "copyAssets" && key !== "copyDataAssets") {
            const messageDiv = document.createElement("div");
            messageDiv.className = "validation-message";
            messageDiv.id = `${key}-validation`;
            input.parentNode?.insertBefore(messageDiv, input.nextSibling);
        }
    });

    // Track if fields have been touched
    const touchedFields = new Set<string>();

    function validateForm(showMessages: boolean = false): ValidationMessage[] {
        const errors: ValidationMessage[] = [];
        const inputApiKey = elements.inputFields.apiKey.value.trim();
        const outputApiKey = elements.inputFields.apiKeyOutput.value.trim();

        // Clear existing validation messages and styles
        document.querySelectorAll(".validation-message").forEach((el) => {
            el.textContent = "";
            el.classList.remove("show");
        });
        document.querySelectorAll("input").forEach((input) => {
            input.classList.remove("error");
        });

        // Required field validation - only show errors for touched fields
        Object.entries(elements.inputFields).forEach(([key, input]) => {
            if (
                key !== "copyAssets" &&
                key !== "copyDataAssets" &&
                !input.value.trim() &&
                (showMessages || touchedFields.has(key))
            ) {
                errors.push({
                    field: key,
                    message: `${input.previousElementSibling?.textContent?.replace(
                        ":",
                        ""
                    )} is required`,
                });
            }
        });

        // API key validation - only when both fields have values
        if (inputApiKey && outputApiKey) {
            if (inputApiKey === outputApiKey) {
                errors.push({
                    field: "apiKey",
                    message: "Input and Output API Keys must be different",
                });
                errors.push({
                    field: "apiKeyOutput",
                    message: "Input and Output API Keys must be different",
                });
                if (
                    showMessages ||
                    (touchedFields.has("apiKey") &&
                        touchedFields.has("apiKeyOutput"))
                ) {
                    elements.inputFields.apiKey.classList.add("error");
                    elements.inputFields.apiKeyOutput.classList.add("error");
                }
            }
        }

        // Only show validation messages if showMessages is true or the field has been touched
        errors.forEach((error) => {
            const messageEl = document.getElementById(
                `${error.field}-validation`
            );
            if (messageEl && (showMessages || touchedFields.has(error.field))) {
                messageEl.textContent = error.message;
                messageEl.classList.add("show");
            }
        });

        return errors;
    }

    // Add validation styles
    const style = document.createElement("style");
    style.textContent = `
        .validation-message {
            color: #dc2626;
            font-size: 0.875rem;
            margin-top: 4px;
            margin-bottom: 10px;
            display: none;
            opacity: 0;
            transition: all 0.2s ease-in-out;
        }

        .validation-message.show {
            display: block;
            opacity: 1;
        }

        input.error {
            border-color: #dc2626 !important;
            background-color: #fef2f2 !important;
            transition: all 0.2s ease-in-out;
        }

        input.error:focus {
            border-color: #dc2626 !important;
            box-shadow: 0 0 0 1px #dc2626 !important;
            outline: none;
        }
    `;
    document.head.appendChild(style);

    // Real-time validation
    Object.entries(elements.inputFields).forEach(([key, input]) => {
        input.addEventListener("blur", () => {
            // Mark field as touched when user leaves it
            touchedFields.add(key);
            const errors = validateForm(false);
            elements.submitButton.disabled = errors.length > 0;
        });

        input.addEventListener("input", () => {
            if (touchedFields.has(key)) {
                const errors = validateForm(false);
                elements.submitButton.disabled = errors.length > 0;
            }
        });
    });

    /**
     * Set the loading state of the submit button
     */
    function setSubmitButtonState(isLoading: boolean): void {
        elements.submitButton.disabled = isLoading;
        const buttonText = elements.submitButton.querySelector(".button-text");
        const buttonSpinner = elements.submitButton.querySelector(
            ".button-spinner"
        ) as HTMLElement;
        if (buttonText)
            buttonText.textContent = isLoading ? "Processing..." : "Submit";
        if (buttonSpinner)
            buttonSpinner.style.display = isLoading ? "inline-block" : "none";
    }

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

        // Enable/disable submit button based on status type
        setSubmitButtonState(type === "progress");
    }

    // Socket.IO event handlers
    socket.on("connect", () => {
        console.log("Connected to server");
        setSubmitButtonState(false);
    });

    socket.on("disconnect", () => {
        console.log("Disconnected from server");
        updateStatus("Connection lost", "Attempting to reconnect...", "error");
        setSubmitButtonState(false);
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

        // Re-enable the submit button on completion
        if (data.type === "success" || data.type === "error") {
            setSubmitButtonState(false);
        }
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

    // Initial form validation
    elements.submitButton.disabled = true;

    // Form submission
    elements.form.addEventListener("submit", (event) => {
        event.preventDefault();
        // Mark all fields as touched when attempting to submit
        Object.keys(elements.inputFields).forEach((key) =>
            touchedFields.add(key)
        );
        const errors = validateForm(true); // Show all validation messages on submit

        if (errors.length > 0) {
            elements.submitButton.disabled = true;
            return;
        }

        const formData = getFormData();
        socket.emit("start_copy", formData);
        setSubmitButtonState(true);
        updateStatus("Starting", "Initializing copy process...", "progress");
    });
});
