export default {
	generarExpresionUpdate(filaActualizada, filaOriginal) {
		const UpdateExpressionParts = [];
		const ExpressionAttributeNames = {};
		const ExpressionAttributeValues = {};

		for (let key in filaActualizada) {
			if (key === "codigo_contrato" || key === "codigo_de_cuota") continue;

			const nuevoValor = filaActualizada[key];
			// const valorAnterior = filaOriginal[key];

			// if (JSON.stringify(nuevoValor) === JSON.stringify(valorAnterior)) continue;

			const nombreCampo = `#${key}`;
			const valorCampo = `:${key}`;
			UpdateExpressionParts.push(`${nombreCampo} = ${valorCampo}`);
			ExpressionAttributeNames[nombreCampo] = key;

			if (!isNaN(nuevoValor) && nuevoValor !== "") {
				ExpressionAttributeValues[valorCampo] = { N: String(nuevoValor) };
			} else {
				ExpressionAttributeValues[valorCampo] = { S: String(nuevoValor) };
			}
		}

		const UpdateExpression = "SET " + UpdateExpressionParts.join(", ");

		return {
			UpdateExpression,
			ExpressionAttributeNames,
			ExpressionAttributeValues,
		};
	},

	actualizarItem() {
		let filaActualizada = Table2.updatedRow;
		const filaOriginal = Table2.triggeredRow;

		if (!filaActualizada.codigo_contrato || !filaActualizada.codigo_de_cuota) {
			// showAlert("Faltan claves primarias", "error");
			return {};
		}

		// Aplicar fórmulas
		filaActualizada = Utils.calcularCamposFormula(filaActualizada);

		const expresiones = this.generarExpresionUpdate(filaActualizada, filaOriginal);

		return {
			TableName: "poc-bd-pagos",
			Key: {
				codigo_contrato: { S: String(filaActualizada.codigo_contrato) },
				codigo_de_cuota: { S: String(filaActualizada.codigo_de_cuota) }
			},
			UpdateExpression: expresiones.UpdateExpression,
			ExpressionAttributeNames: expresiones.ExpressionAttributeNames,
			ExpressionAttributeValues: expresiones.ExpressionAttributeValues
		};
	},

	// Reordenar
	datosReordenados() {
		const items = QueryDynamo.data.Items;
		if (!items || items.length === 0) return [];

		// Orden de columnas deseado
		const ordenColumnas = [
			"codigo_de_cuota",
			"codigo_operacion",
			"codigo_contrato",
			"codigo_empresario",
			"empresario",
			"codigo_inversionista",
			"inversionista",
			"tipo_de_prestamo",
			"situacion_del_credito",
			"distrito",
			"cuota_esperada_mensual",
			"nro_cuotas",
			"fecha_de_pago_esperada_original",
			"saldo_por_cancelar",
			"interes_esperado_fraccionado_original",
			"amortizacion_esperada_fraccionado_original",
			"interes_esperado_original",
			"amortizacion_esperada_original",
			"igv_esperada_original",
			"cuota_esperada_actualizada",
			"saldo_por_cancelar_esperado_actualizada",
			"capital_fraccionado_actualizado",
			"interes_fraccionado_actualizado",
			"interes_esperado_actualizado",
			"amortizacion_esperada_actualizado",
			"igv_esperada_actualizado",
			"fecha_de_pago_del_cliente",
			"monto_total_pagado_al_credito",
			"capital_fraccionado_pagado",
			"interes_fraccionado_pagado",
			"capital_pagado",
			"interes_pagado",
			"igv_pagado",
			"descuento_de_interes",
			"descuento_de_capital",
			"monto_ampliado_renovado_o_sustituido",
			"penalidades",
			"saldo_a_favor",
			"dias_de_atraso_de_pago",
			"status",
			"dias_de_adelanto_de_pago",
			"seguimiento_de_pagos",
			"condicion_actual_del_credito",
			"detalle_de_condicion",
			"condicion_asignada",
			"causal_de_cancelacion",
			"medio_de_cancelacion",
			"puntualidad",
			"moneda",
			"fondo",
			"prestamo_es_fondeado_por_swap",
			"estado_de_prestamo",
			"f_de_amortizacion",
			"producto"
		];

		return items.map((item) => {
			const nuevaFila = {};

			// Primero agrega columnas en orden definido
			ordenColumnas.forEach((col) => {
				if (col in item) {
					nuevaFila[col] = item[col];
				}
			});

			// Luego agrega el resto de columnas no incluidas en el orden
			Object.keys(item).forEach((col) => {
				if (!(col in nuevaFila)) {
					nuevaFila[col] = item[col];
				}
			});

			return nuevaFila;
		});
	},

	// Procesar CSV
	procesarArchivo: async (file) => {
		if (!file || typeof file.data !== "string") {
			// showAlert("Archivo inválido o no es texto plano", "error");
			return [];
		}

		const contenido = file.data;
		const lineas = contenido.trim().split("\n");
		const headers = lineas[0].split(";").map(h => h.trim());

		// Lista de columnas que deben ser fechas
		const columnasFecha = [
			"fecha_de_pago_del_cliente",
			"fecha_de_pago_esperada_original",
			"f_de_amortizacion"
		];

		// Helper: validar y normalizar fechas
		function normalizarFecha(fechaStr, colName, rowIndex) {
			if (!fechaStr || fechaStr.trim() === "") return null;

			const fechaLimpia = fechaStr.trim();

			// Caso 1: formato YYYY-MM-DD
			const regexISO = /^\d{4}-\d{2}-\d{2}$/;
			if (regexISO.test(fechaLimpia)) {
				return fechaLimpia;
			}

			// Caso 2: formato DD/MM/YYYY → convertir a YYYY-MM-DD
			const regexLatam = /^\d{2}\/\d{2}\/\d{4}$/;
			if (regexLatam.test(fechaLimpia)) {
				const [dia, mes, anio] = fechaLimpia.split("/").map(Number);
				return `${anio}-${String(mes).padStart(2, "0")}-${String(dia).padStart(2, "0")}`;
			}

			// Ningún formato válido
			throw new Error(
				`Formato inválido en columna "${colName}", fila ${rowIndex + 2}: "${fechaStr}". Se espera YYYY-MM-DD o DD/MM/YYYY`
			);
		}

		const data = lineas.slice(1).map((linea, rowIndex) => {
			const valores = linea.split(";").map(v => v.trim());
			const obj = {};

			headers.forEach((h, i) => {
				let valor = valores[i] ?? null;

				if (columnasFecha.includes(h)) {
					valor = normalizarFecha(valor, h, rowIndex);
				}

				obj[h] = valor;
			});

			return Utils.calcularCamposFormula(obj);
		});

		storeValue("preview_data", data);
		return data;
	},

	// Carga de registros en dynamo
	esquemaDynamo: {
		codigo_de_cuota: "S",
		codigo_operacion: "S",
		codigo_contrato: "S",
		codigo_empresario: "S",
		empresario: "S",
		codigo_inversionista: "S",
		inversionista: "S",
		tipo_de_prestamo: "S",
		situacion_del_credito: "S",
		distrito: "S",
		cuota_esperada_mensual: "N",
		nro_cuotas: "N",
		fecha_de_pago_esperada_original: "S",
		saldo_por_cancelar: "N",
		interes_esperado_fraccionado_original: "N",
		amortizacion_esperada_fraccionado_original: "N",
		interes_esperado_original: "N",
		amortizacion_esperada_original: "N",
		igv_esperada_original: "N",
		cuota_esperada_actualizada: "N",
		saldo_por_cancelar_esperado_actualizada: "N",
		capital_fraccionado_actualizado: "N",
		interes_fraccionado_actualizado: "N",
		interes_esperado_actualizado: "N",
		amortizacion_esperada_actualizado: "N",
		igv_esperada_actualizado: "N",
		fecha_de_pago_del_cliente: "S",
		monto_total_pagado_al_credito: "N",
		capital_fraccionado_pagado: "N",
		interes_fraccionado_pagado: "N",
		capital_pagado: "N",
		interes_pagado: "N",
		igv_pagado: "N",
		descuento_de_interes: "N",
		descuento_de_capital: "N",
		monto_ampliado_renovado_o_sustituido: "N",
		penalidades: "N",
		saldo_a_favor: "N",
		dias_de_atraso_de_pago: "N",
		status: "S",
		dias_de_adelanto_de_pago: "N",
		seguimiento_de_pagos: "S",
		condicion_actual_del_credito: "S",
		detalle_de_condicion: "S",
		condicion_asignada: "S",
		causal_de_cancelacion: "S",
		medio_de_cancelacion: "S",
		puntualidad: "S",
		moneda: "S",
		fondo: "S",
		prestamo_es_fondeado_por_swap: "S",
		estado_de_prestamo: "S",
		f_de_amortizacion: "S",
		producto: "S"
	},

	prepararItem(item) {
		const output = {};
		for (let key in item) {
			const tipo = this.esquemaDynamo[key];
			const valor = item[key];

			if (valor === "" || valor === null || valor === undefined) {
				if (tipo === "S") {
					// Strings vacíos o nulos -> espacio en blanco
					output[key] = { S: " " };
				} else if (tipo === "N") {
					// Números vacíos o nulos -> 0
					output[key] = { N: "0" };
				}
				continue;
			}

			if (tipo === "S") {
				const texto = valor.toString().trim();
				// Si después del trim queda vacío -> guardar espacio
				output[key] = { S: texto === "" ? " " : texto };
			} else if (tipo === "N") {
				const numero = Number(valor);
				if (!isNaN(numero)) {
					output[key] = { N: numero.toString() };
				} else {
					// fallback si no es número válido
					output[key] = { N: "0" };
				}
			}
		}
		return output;
	},

	prepararItems(items) {
		return items.map((item) => ({
			PutRequest: {
				Item: this.prepararItem(item)
			}
		}));
	},

	cargarDynamo() {
		const data = appsmith.store.preview_data;
		if (!Array.isArray(data) || data.length === 0) {
			return [];
		}

		return this.prepararItems(data);
	},

	dividirEnChunks(array, tamano) {
		const chunks = [];
		for (let i = 0; i < array.length; i += tamano) {
			chunks.push(array.slice(i, i + tamano));
		}
		return chunks;
	},

	subirItemsPorLotes: async () => {
		try {
			const allItems = Utils.cargarDynamo();
			const chunks = Utils.dividirEnChunks(allItems, 25);

			for (let i = 0; i < chunks.length; i++) {
				const lote = chunks[i];

				await storeValue("lote_actual", lote);

				// Ejecutar el query y capturar si falla
				try {
					const response = await BatchWriteItemDynamo.run();

					// Validar que no haya UnprocessedItems
					if (
						response.UnprocessedItems &&
						Object.keys(response.UnprocessedItems).length > 0
					) {
						throw new Error("Algunos ítems no se procesaron en el lote " + (i + 1));
					}

				} catch (error) {
					console.error("Error en lote:", i + 1, error);
					showAlert("Error al guardar lote " + (i + 1), "error");
					throw error; // Detiene la ejecución del resto
				}
			}

			showAlert("Carga completada con éxito", "success");

		} catch (err) {
			console.error("Fallo general en subirItemsPorLotes", err);
			showAlert("La carga falló: " + err.message, "error");
		}
	},

	normalizarFecha(fechaStr) {
		if (!fechaStr || fechaStr.trim() === "") return null;
		const f = new Date(fechaStr.trim()); 
		f.setHours(0, 0, 0, 0); // normaliza a medianoche local
		return f;
	},

	// Calculo de columnas
	calcularCamposFormula: (item) => {
		const hoy = new Date(); // Fecha actual de usuario que ejecuta

		const fechaPagoClienteRaw = Utils.normalizarFecha(item.fecha_de_pago_del_cliente);
		const fechaEsperadaRaw = Utils.normalizarFecha(item.fecha_de_pago_esperada_original);

		const fechaPagoCliente = fechaPagoClienteRaw ? new Date(fechaPagoClienteRaw) : null;
		const fechaEsperada = fechaEsperadaRaw ? new Date(fechaEsperadaRaw) : null;

		// Campo: status
		let status = item.status;
		if (!status || status === "") {
			if (fechaEsperada && hoy >= fechaEsperada) {
				status = "SEGUIMIENTO";
			} else {
				status = "TODAVIA NO VENCE";
			}
		}

		// Campo: dias_de_atraso_de_pago
		let diasAtrasoDePago = 0;
		if (!fechaPagoCliente && status === "SEGUIMIENTO") {
			diasAtrasoDePago = Math.floor((hoy - fechaEsperada) / (1000 * 60 * 60 * 24));
		} else if (fechaPagoCliente && fechaEsperada) {
			const diferencia = Math.floor((fechaPagoCliente - fechaEsperada) / (1000 * 60 * 60 * 24));
			diasAtrasoDePago = diferencia > 0 ? diferencia : 0;
		}

		// Campo: dias_de_adelanto_de_pago
		let diasAdelantoDePago = 0;
		if (fechaPagoCliente && fechaEsperada) {
			const diferencia = fechaPagoCliente - fechaEsperada;
			if (diferencia < 0) {
				diasAdelantoDePago = Math.floor((fechaEsperada - fechaPagoCliente) / (1000 * 60 * 60 * 24));
			}
		}

		// Campo: detalle_de_condicion
		let detalleCondicion = item.condicion_asignada && item.condicion_asignada.trim() !== "" ? item.condicion_asignada: item.puntualidad;

		return {
			...item,
			status: status,
			dias_de_atraso_de_pago: diasAtrasoDePago,
			dias_de_adelanto_de_pago: diasAdelantoDePago,
			detalle_de_condicion: detalleCondicion
		};
	},

};