import "./style.css";

import * as wasm from "jelly_web";
import { Writer } from "n3";
import { DataFactory } from "n3";


let checked = false;

const checkbox = document.getElementById("showQuadsCheckbox");

checkbox.addEventListener("change", () => {
    if (checkbox.checked) {
        checked = true;
    } else {
        checked = true;
    }
});
checkbox.addEventListener("load", () => {
    checked = checkbox.checked;
})

/**
    * @param {HTMLDivElement} element 
    * @param {string} name 
    */
function createFile(element, name) {
    const wrapper = document.createElement("div");
    wrapper.className = "file";

    const title = document.createElement("header");

    const nameEl = document.createElement("span");
    nameEl.innerText = name;

    const tcount = document.createElement("h3");
    tcount.innerText = 0 + "";

    title.append(tcount, nameEl);

    wrapper.appendChild(title);
    const frames = document.createElement("div");
    frames.className = "frames"
    wrapper.appendChild(frames);

    element.appendChild(wrapper);
    return {
        frames,
        tcount,
        wrapper,
        time: 0,
        frameCount: 0,
    };
}

const df = DataFactory;
const outputField = document.getElementById("output");

function collapsable(input, title) {
    const wrapper = document.createElement("div");
    wrapper.className = "collapsible";

    const pre = document.createElement("pre");
    pre.innerText = input;
    const button = document.createElement("div")
    button.innerText = title + " Show more";
    button.className = "collapsible-toggle";

    button.addEventListener("click", () => {
        wrapper.classList.toggle("expanded");
        button.textContent = wrapper.classList.contains("expanded")
            ? title + " Show less"
            : title + " Show more";
    });

    wrapper.append(button, pre);
    return wrapper;
}


async function readFile(file) {
    const name = file instanceof File ? file.name : file;
    const elements = createFile(outputField, name);

    const reader = new wasm.FrameReader();

    const sender = reader.sender();
    const handler = new wasm.Handler(df, reader);

    let quadCount = 0;
    streamSource(file, (chunk) => {
        sender.send(chunk)
    }).then(() => {
        sender.close()
    });

    let list = [];
    const h = {
        onQuad: (q) => {
            quadCount += 1;
            list.push(q);
        }
    };

    while (true) {
        const start = performance.now();
        const notFinished = await handler.next_frame(h);
        const end = performance.now();

        if (!notFinished) {
            break;
        }


        elements.frameCount += 1;
        elements.time += end - start;

        if (checked) {
            const st = new Writer().quadsToString(list);
            const child = collapsable(st, `Took ${end - start} ms ${list.length} quads`)
            elements.frames.appendChild(child)
        }
        elements.tcount.innerText = `${quadCount} triples in ${elements.time} ms (${elements.frameCount} frames) (${(quadCount / elements.time).toFixed(2)} quads per ms)`;
        list = []
    }
}

async function streamSource(source, cb) {
    if (source instanceof File) {
        // Read local file chunkwise
        const reader = source.stream().getReader();
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            cb(value)
        }
    } else if (typeof source === "string") {
        // Fetch remote file chunkwise
        const resp = await fetch(source);
        if (!resp.ok) throw new Error(`Failed to fetch ${source}: ${resp.status}`);
        const reader = resp.body.getReader();
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            cb(value)
        }
    } else {
        throw new Error("Unsupported source type");
    }
}

window.streamSource = readFile;
