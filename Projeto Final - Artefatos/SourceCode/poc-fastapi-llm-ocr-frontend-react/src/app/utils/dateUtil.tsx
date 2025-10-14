export function isValidDateBR(dateStr) {
    // 1. Confere o padrão com regex
    const regex = /^([0-2]\d|3[0-1])\/(0\d|1[0-2])\/\d{4}$/;
    if (!regex.test(dateStr)) return false;

    // 2. Quebra a string em partes
    const [dayStr, monthStr, yearStr] = dateStr.split("/");
    const day = parseInt(dayStr, 10);
    const month = parseInt(monthStr, 10);
    const year = parseInt(yearStr, 10);

    // 3. Cria objeto Date (mês começa do 0 em JS)
    const date = new Date(year, month - 1, day);

    // 4. Verifica se os valores batem (evita 31/02/2025, etc.)
    return (
        date.getFullYear() === year &&
        date.getMonth() === month - 1 &&
        date.getDate() === day
    );
}


export function formatDateBRtoISO(dateStr) {
    // Confere padrão básico DD/MM/YYYY
    const regex = /^([0-2]\d|3[0-1])\/(0\d|1[0-2])\/\d{4}$/;
    if (!regex.test(dateStr)) {
        throw new Error("Data inválida. Use o formato DD/MM/YYYY.");
    }

    const [day, month, year] = dateStr.split("/");

    // Retorna no formato YYYY-MM-DD
    return `${year}-${month}-${day}`;
}