const fs = require('fs');

const pdfjs = require('pdfjs-dist/build/pdf');

pdfjs.disableWorker = true;

INPUT_FILE = process.argv[2];
OUTPUT_FILE = process.argv[3];
OUTPUT_STREAM = fs.createWriteStream(OUTPUT_FILE, {flags : 'w'});

async function processPDF() {
     const buffer = fs.readFileSync(INPUT_FILE);
     const loadingTask = pdfjs.getDocument({ data: buffer });
     // const loadingTask = pdfjs.getDocument({ url: INPUT_FILE });

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
    OUTPUT_STREAM.write('[')
    for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
        const pagePromise = doc.getPage(pageNum).then(async (page) => {
            const textContent = await page.getTextContent()
            const text = textContent.items
                .map((item) => (item.hasEOL ? `${item.str} ` : item.str))
                .join('')
            return { pageNum, text }
        })
        if (pageNum > 1) {
            OUTPUT_STREAM.write(',')
        }
        OUTPUT_STREAM.write(JSON.stringify(await pagePromise));
    }
    OUTPUT_STREAM.write(']')
}

processPDF();
