const fs = require('fs');

const vendors = ["Rami Levy - Hashikma Marketing", "Mega", "Shufersal", "Hazi-Hinam", "Osher ad",
          "Victory", "Tiv-Taam", "AM:PM"]

const products = [{
    id: "milk 1%",
    price: 5.59
  },
  {
    id: "milk 3%",
    price: 5.94
  },
  {
    id: "white bread",
    price: 5.12
  },
  {
    id: "brown bread",
    price: 5.12
  },
  {
    id: "butter",
    price: 3.94
  },
  {
    id: "white cheese",
    price: 4.75
  },
  {
    id: "eggs XL",
    price: 12.4
  },
  {
    id: "eggs L",
    price: 11.3
  },
  {
    id: "eggs M",
    price: 10.4
  },
  {
    id: "kotej",
    price: 5.7
  },
  {
    id: "onion 1kg",
    price: 6.9
  },
  {
    id: "tester choise",
    price: 29.9
  },
  {
    id: "banana 1kg",
    price: 12.9
  },
  {
    id: "milki",
    price: 2.9
  },
  {
    id: "sugar",
    price: 5.5
  },
  {
    id: "tuna",
    price: 24.9
  },
  {
    id: "pasta",
    price: 7.5
  },
  {
    id: "ketchup",
    price: 10.9
  },
  {
    id: "sweet corn",
    price: 6.4
  },
  {
    id: "Coca Cola Zero",
    price: 36.9
  },
  {
    id: "humus",
    price: 14.9
  },
  {
    id: "chiken 1kg",
    price: 24.9
  },
  {
    id: "Water 6pc",
    price: 11.5
  },
  {
    id: "potato",
    price: 5.9
  },
  {
    id: "salmon 1kg",
    price: 99.9
  }
]

let i =35;
while (i < 51) {

  let invoice = {
    invoiceNumber: "45"+i,
    date: randomDate(new Date(2019, 0, 1), new Date()),
    provider: vendors[Math.floor(Math.random() * vendors.length)],
    items: [
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)],
      products[Math.floor(Math.random() * products.length)]
    ]
  }

  invoice["total"] = getTotalPrice(invoice["items"])

  fs.writeFile("../invoices/invoice"+i+".json", JSON.stringify(invoice), function(err) {
    if (err) throw err;
  })
    i++;
}

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function getTotalPrice(items) {
  let total = 0;
  for (const item of items) {
    total += item["price"]
  }
  return total.toFixed(2);
}
