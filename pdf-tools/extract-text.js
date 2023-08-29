const fs = require('fs');

const pdfjs = require('pdfjs-dist/build/pdf');

pdfjs.disableWorker = true;

async function processPDF(filePath) {
    // const buffer = fs.readFileSync(filePath);
    // const loadingTask = pdfjs.getDocument({ data: buffer });
    const loadingTask = pdfjs.getDocument({ url: filePath });

    loadingTask.promise.then(
        async (doc) => {
            await extractTextContent(doc);
        },
        (error) => {
            console.error(error);
            process.exit(1);
        }
    );
}

const extractTextContent = async (doc) => {
    console.log('[')
    for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
        const pagePromise = doc.getPage(pageNum).then(async (page) => {
            const textContent = await page.getTextContent()
            const text = textContent.items
                .map((item) => (item.hasEOL ? `${item.str} ` : item.str))
                .join('')
                .toLowerCase()
            return { pageNum, text }
        })
        if (pageNum > 1) {
            console.log(',')
        }
        console.log(JSON.stringify(await pagePromise));
    }
    console.log(']')
}

processPDF(process.argv[2]);
