const fs = require('fs');

const pdfjs = require('pdfjs-dist/build/pdf');

pdfjs.GlobalWorkerOptions.workerSrc = 'tmp/pdf.worker.js';

async function processPDF(filePath) {
    const buffer = fs.readFileSync(filePath);
    const loadingTask = pdfjs.getDocument({ data: buffer });

    loadingTask.promise.then(
        async (doc) => {
            const data = await extractTextContent(doc);
            return data;
        },
        (error) => {
            console.error(error);
            process.exit(1);
        }
    );
}

const extractTextContent = async (doc) => {
    const pagePromises = []

    for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
        const pagePromise = doc.getPage(pageNum).then(async (page) => {
            const textContent = await page.getTextContent()
            const text = textContent.items
                .map((item) => (item.hasEOL ? `${item.str} ` : item.str))
                .join('')
                .toLowerCase()
            return { pageNum, text }
        })
        pagePromises.push(pagePromise)
    }

    return await Promise.all(pagePromises)
}

processPDF(process.argv[2]);
