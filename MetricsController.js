const express = require("express");
require("dotenv").config();
const app = express();
const { v4: uuidv4 } = require("uuid");
const axios = require("axios");
const qs = require("qs");
const csvParser = require("csv-parser");
const request = require("request");
const util = require("util");
const groupBy = (x, f) => x.reduce((a, b, i) => ((a[f(b, i, x)] ||= []).push(b), a), {});
const { adx_voluum_fb_merged_daily_data, adx_voluum_fb_merged_hourly_data, adx_voluum_daily_data, adx_fb_daily_data, adx_fb_user_preset_filters, adx_user, adx_fb_ad_accounts } = require("./../config/models");

let saveFilters = async (filters, userId, filterGroupName) => {
	return new Promise(async (resolve, reject) => {
		try {
			if (filterGroupName == "none" || userId == "none") {
				resolve("done");
			} else {
				let filterGroupNameIdLowercase = filterGroupName.toLowerCase();
				let filterGroupNameId = filterGroupNameIdLowercase.replace(" ", "_");
				let filterId = process.env.TrafficSource + "_" + process.env.ModuleName + "_" + userId + "_" + filterGroupNameId;
				let filterData = {};
				filterData.id = filterId;
				filterData.name = filterGroupName;
				filterData.user_id = userId;
				filterData.status = "Active";
				filterData.moduleName = process.env.ModuleName;
				filterData.trafficSource = process.env.TrafficSource;
				filterData.filters = filters;
				await adx_fb_user_preset_filters.bulkWrite([
					{
						updateOne: {
							filter: { _id: filterId },
							update: filterData,
							upsert: true,
						},
					},
				]);
				resolve("done");
			}
		} catch (e) {
			console.trace(e);
		}
	});
};
let getFilter = async (userId) => {
	return new Promise(async (resolve, reject) => {
		try {
			let responseList = [];

			let filters = await adx_fb_user_preset_filters.findOne({ user_id: userId, moduleName: process.env.ModuleName });
			resolve(filters);
		} catch (e) {
			console.trace(e);
		}
	});
};

// let getPrimaryAdAccounts = async (mediaBuyerUserName) => {
// 	return new Promise(async (resolve, reject) => {
// 		try {
// 			let primaryAdAccounts = await adx_fb_ad_accounts.find({ primary_media_buyer: mediaBuyerUserName }).lean();
// 			resolve(primaryAdAccounts);
// 		} catch (e) {
// 			console.trace(e);
// 		}
// 	});
// };
const getPrimaryAdAccounts = async (mediaBuyerUserName) => {
	try {
		const primaryAdAccounts = await adx_fb_ad_accounts.find({ primary_media_buyer: mediaBuyerUserName }).lean();
		return primaryAdAccounts;
	} catch (error) {
		console.error(error);
		throw error;
	}
};

let getAggreagtedMediaBuyerAccountData = async (aggregatePipeline) => {
	return new Promise(async (resolve, reject) => {
		try {
			let aggreagtedMediaBuyerAccountData = await adx_voluum_fb_merged_hourly_data.aggregate(aggregatePipeline);
			resolve(aggreagtedMediaBuyerAccountData);
		} catch (e) {
			if (e.response) {
				console.log(e.response.data.error);
				console.log(e.response.status);
				console.log(e.response.statusText);
			} else if (e.request) {
				console.log(e.request);
			} else {
				console.log("error ", e.message);
			}
		}
	});
};

// let getMediaBuyerData = async (mediaBuyer, firstMatch, sortObject, skip, limit) => {
// 	return new Promise(async (resolve, reject) => {
// 		try {
// 			let primaryAdAccounts = [];
// 			primaryAdAccounts = await getPrimaryAdAccounts(mediaBuyer.userName);
// 			if (primaryAdAccounts.length == 0) {
// 				resolve(false);
// 				return;
// 			}
// 			primaryAdAccounts.length == 0 ? resolve(false) : (t = 0);
// 			let adAccounts = [];
// 			await Promise.all(
// 				primaryAdAccounts.map(async (adAccount) => {
// 					adAccounts.push(adAccount._id);
// 				}),
// 			);
// 			firstMatch.accountId = {
// 				$in: adAccounts,
// 			};
// 			console.log("mediaBuyer: " + mediaBuyer.userName);
// 			/*console.log("adAccounts: ", adAccounts);
// 			console.log("firstMatch: ", firstMatch);*/
// 			let aggregatePipeline = [];
// 			aggregatePipeline.push({ $match: firstMatch });
// 			let groupBy = {
// 				$group: {
// 					_id: "$accountId",
// 					fbClicks: {
// 						$sum: "$fbClicks",
// 					},
// 					fbImpressions: {
// 						$sum: "$fbImpressions",
// 					},
// 					fbLinkClicks: {
// 						$sum: "$fbLinkClicks",
// 					},
// 					fbConversions: {
// 						$sum: "$fbConversions",
// 					},
// 					fbSpend: {
// 						$sum: "$fbSpend",
// 					},
// 					fbSpendWithTaxes: {
// 						$sum: "$fbSpendWithTaxes",
// 					},
// 					voluumVisits: {
// 						$sum: "$voluumVisits",
// 					},
// 					voluumClicks: {
// 						$sum: "$voluumClicks",
// 					},
// 					voluumConversions: {
// 						$sum: "$voluumConversions",
// 					},
// 					volRevenue: {
// 						$sum: "$volRevenue",
// 					},
// 					volCost: {
// 						$sum: "$volCost",
// 					},
// 					voluumCpv: {
// 						$sum: "$voluumCpv",
// 					},
// 					accountName: {
// 						$first: "$accountName",
// 					},
// 					accountId: {
// 						$first: "$accountId",
// 					},
// 				},
// 			};
// 			aggregatePipeline.push(groupBy);
// 			console.log("aggregatePipeline: ", util.inspect(aggregatePipeline, false, null, true));
// 			let aggreagtedMediaBuyerAccountData = await getAggreagtedMediaBuyerAccountData(aggregatePipeline);
// 			console.log("aggreagtedMediaBuyerAccountData: ", aggreagtedMediaBuyerAccountData);
// 			let metrics = {
// 				_id:[]
// 			};
// 			await Promise.all(
// 				aggreagtedMediaBuyerAccountData.map(async (data) => {
// 					metrics._id.push(data._id);
// 					typeof metrics.fbImpressions !== "undefined" && metrics.fbImpressions !== undefined
// 						? (metrics.fbImpressions += data.fbImpressions ?? 0)
// 						: (metrics.fbImpressions = data.fbImpressions ?? 0);
// 					typeof metrics.fbClicks !== "undefined" && metrics.fbClicks !== undefined ? (metrics.fbClicks += data.fbClicks ?? 0) : (metrics.fbClicks = data.fbClicks ?? 0);
// 					typeof metrics.fbLinkClicks !== "undefined" && metrics.fbLinkClicks !== undefined
// 						? (metrics.fbLinkClicks += data.fbLinkClicks ?? 0)
// 						: (metrics.fbLinkClicks = data.fbLinkClicks ?? 0);
// 					typeof metrics.fbConversions !== "undefined" && metrics.fbConversions !== undefined
// 						? (metrics.fbConversions += data.fbConversions ?? 0)
// 						: (metrics.fbConversions = data.fbConversions ?? 0);
// 					typeof metrics.fbSpend !== "undefined" && metrics.fbSpend !== undefined ? (metrics.fbSpend += data.fbSpend ?? 0) : (metrics.fbSpend = data.fbSpend ?? 0);
// 					typeof metrics.fbSpendWithTaxes !== "undefined" && metrics.fbSpendWithTaxes !== undefined
// 						? (metrics.fbSpendWithTaxes += data.fbSpendWithTaxes ?? 0)
// 						: (metrics.fbSpendWithTaxes = data.fbSpendWithTaxes ?? 0);
// 					typeof metrics.voluumVisits !== "undefined" && metrics.voluumVisits !== undefined
// 						? (metrics.voluumVisits += data.voluumVisits ?? 0)
// 						: (metrics.voluumVisits = data.voluumVisits ?? 0);
// 					typeof metrics.voluumClicks !== "undefined" && metrics.voluumClicks !== undefined
// 						? (metrics.voluumClicks += data.voluumClicks ?? 0)
// 						: (metrics.voluumClicks = data.voluumClicks ?? 0);
// 					typeof metrics.voluumConversions !== "undefined" && metrics.voluumConversions !== undefined
// 						? (metrics.voluumConversions += data.voluumConversions ?? 0)
// 						: (metrics.voluumConversions = data.voluumConversions ?? 0);
// 					typeof metrics.volRevenue !== "undefined" && metrics.volRevenue !== undefined ? (metrics.volRevenue += data.volRevenue ?? 0) : (metrics.volRevenue = data.volRevenue ?? 0);
// 					typeof metrics.volCost !== "undefined" && metrics.volCost !== undefined ? (metrics.volCost += data.volCost ?? 0) : (metrics.volCost = data.volCost ?? 0);
// 					typeof metrics.voluumCpv !== "undefined" && metrics.voluumCpv !== undefined ? (metrics.voluumCpv += data.voluumCpv ?? 0) : (metrics.voluumCpv = data.voluumCpv ?? 0);
// 				}),
// 			);
// 			metrics.fbCpm = metrics.fbImpressions != 0 ? (metrics.fbSpend / metrics.fbImpressions) * 1000 : 0;
// 			metrics.fbCpc = metrics.fbLinkClicks != 0 ? metrics.fbSpend / metrics.fbLinkClicks : 0;
// 			metrics.fbCtr = metrics.fbImpressions != 0 ? (metrics.fbLinkClicks / metrics.fbImpressions) * 100 : 0;
// 			metrics.voluumCr = metrics.voluumClicks != 0 ? (metrics.voluumConversions / metrics.voluumClicks) * 100 : 0;
// 			metrics.voluumEpc = metrics.voluumClicks != 0 ? metrics.volRevenue / metrics.voluumClicks : 0;
// 			metrics.voluumEpv = metrics.voluumVisits != 0 ? metrics.volRevenue / metrics.voluumVisits : 0;
// 			metrics.voluumProfit = metrics.volRevenue - metrics.fbSpendWithTaxes;
// 			metrics.voluumCtr = metrics.voluumVisits != 0 ? (metrics.voluumClicks / metrics.voluumVisits) * 100 : 0;
// 			metrics.voluumAp = metrics.voluumConversions != 0 ? metrics.volRevenue / metrics.voluumConversions : 0;
// 			metrics.roi = metrics.fbSpendWithTaxes != 0 ? (metrics.voluumProfit / metrics.fbSpendWithTaxes) * 100 : 0;
// 			metrics.cpa = metrics.voluumConversions != 0 ? metrics.fbSpendWithTaxes / metrics.voluumConversions : 0;
// 			metrics.clickLoss = metrics.fbLinkClicks - metrics.voluumVisits;
// 			metrics.media_buyer = mediaBuyer.firstName.charAt(0).toUpperCase() + mediaBuyer.firstName.slice(1) + " " + mediaBuyer.lastName.charAt(0).toUpperCase() + mediaBuyer.lastName.slice(1);
// 			resolve(metrics);
// 			console.log("metrics:", metrics);
// 		} catch (e) {
// 			console.trace(e);
// 		}
// 	});
// };
const getMediaBuyerData = async (mediaBuyer, firstMatch, sortObject, skip, limit) => {
	try {
		const primaryAdAccounts = await getPrimaryAdAccounts(mediaBuyer.userName);
		if (primaryAdAccounts.length === 0) {
			return false;
		}

		const adAccounts = primaryAdAccounts.map(adAccount => adAccount._id);
		firstMatch.accountId = {
			$in: adAccounts,
		};

		console.log("mediaBuyer: " + mediaBuyer.userName);
		/*console.log("adAccounts: ", adAccounts);
		console.log("firstMatch: ", firstMatch);*/
		const aggregatePipeline = [
			{ $match: firstMatch },
			{
				$group: {
					_id: "$accountId",
					accountName: { $first: "$accountName" },
					...[
							"fbClicks",
							"fbImpressions",
							"fbLinkClicks",
							"fbConversions",
							"fbSpend",
							"fbSpendWithTaxes",
							"voluumVisits",
							"voluumClicks",
							"voluumConversions",
							"volRevenue",
							"volCost",
							"voluumCpv",
						].reduce((obj, key) => {
							obj[key] = { $sum: `$${key}` };
							return obj;
						}, {}),
				},
			},
		];

		console.log("aggregatePipeline: ", util.inspect(aggregatePipeline, false, null, true));
		const aggregatedMediaBuyerAccountData = await getAggregatedMediaBuyerAccountData(aggregatePipeline);
		console.log("aggregatedMediaBuyerAccountData: ", aggregatedMediaBuyerAccountData);

		const metrics = aggregatedMediaBuyerAccountData.reduce(
			(metrics, data) => {
				metrics._id.push(data._id);
				Object.keys(data).forEach(key => {
					if (key !== "_id" && key !== "accountName") {
						metrics[key] += data[key] ?? 0;
					}
				});
				return metrics;
			},
			{ _id: [] }
		);

		return metrics;
	} catch (error) {
		console.error(error);
		throw error;
	}
};


// let getGroupedData = async (mediaBuyers, firstMatch, sortObject, skip, limit) => {
// 	return new Promise(async (resolve, reject) => {
// 		try {
// 			let lists = [];
// 			await Promise.all(
// 				mediaBuyers.map(async (mediaBuyer) => {
// 					let mediaBuyerData = await getMediaBuyerData(mediaBuyer, firstMatch, sortObject, skip, limit);
// 					if (mediaBuyerData != false) {
// 						lists.push(mediaBuyerData);
// 					}
// 				}),
// 			);
// 			resolve(lists);
// 		} catch (e) {
// 			console.trace(e);
// 		}
// 	});
// };
const getGroupedData = async (mediaBuyers, firstMatch, sortObject, skip, limit) => {
	try {
		const lists = [];
		for (const mediaBuyer of mediaBuyers) {
			const mediaBuyerData = await getMediaBuyerData(mediaBuyer, firstMatch, sortObject, skip, limit);
			if (mediaBuyerData) {
				lists.push(mediaBuyerData);
			}
		}
		return lists;
	} catch (e) {
		console.trace(e);
	}
};

let getSortedArray = async (sortOrder, sortBy, finalMetrics) => {
	return new Promise(async (resolve, reject) => {
		try {
			if (sortOrder == 1) {
				if (sortBy == "media_buyer") {
					await finalMetrics.sort(function (a, b) {
						let fa = a[sortBy].toLowerCase(),
							fb = b[sortBy].toLowerCase();
						if (fa < fb) {
							return -1;
						}
						if (fa > fb) {
							return 1;
						}
						return 0;
					});
					resolve(finalMetrics);
				} else {
					await finalMetrics.sort(function (a, b) {
						let fa = a[sortBy],
							fb = b[sortBy];
						if (fa < fb) {
							return -1;
						}
						if (fa > fb) {
							return 1;
						}
						return 0;
					});
					resolve(finalMetrics);
				}
			} else {
				if (sortBy == "media_buyer") {
					await finalMetrics.sort(function (a, b) {
						let fa = a[sortBy].toLowerCase(),
							fb = b[sortBy].toLowerCase();
						if (fa > fb) {
							return -1;
						}
						if (fa < fb) {
							return 1;
						}
						return 0;
					});
					resolve(finalMetrics);
				} else {
					await finalMetrics.sort(function (a, b) {
						let fa = a[sortBy],
							fb = b[sortBy];
						if (fa > fb) {
							return -1;
						}
						if (fa < fb) {
							return 1;
						}
						return 0;
					});
					resolve(finalMetrics);
				}
			}
		} catch (e) {
			console.trace(e);
		}
	});
};

let getMediaBuyerMetrics = async (req, res) => {
	try {
		req.body.user_id ? (userId = req.body.user_id) : (userId = "none");
		!req.body.filters
			? ((filterobject = {}), (filterobject.filterName = "fbClicks"), (filterobject.filterOperator = "greater_than"), (filterobject.filterValue = -1), (filters = []), filters.push(filterobject))
			: (filters = req.body.filters);
		let startHour;
		let endHour;
		req.body.filterGroupName ? (filterGroupName = req.body.filterGroupName) : (filterGroupName = "none");
		req.body.parentId ? (accountIds = req.body.parentId) : (accountIds = req.body.accountId ?? "0");
		req.body.parentName ? (parentName = req.body.parentName) : (parentName = "account");
		!req.body.startDate ? (startDateObject = new Date()) : (startDateObject = new Date(req.body.startDate));
		!req.body.endDate ? ((endDateObject = new Date()), endDateObject.setDate(endDateObject.getDate() + 1)) : (endDateObject = new Date(req.body.endDate));
		let startDateTimeStamp = startDateObject.getTime();
		let endDateTimeStamp = endDateObject.getTime();
		if (endDateTimeStamp < startDateTimeStamp) {
			return res.json({
				success: false,
				data: "Start Date should be greater than End Date",
			});
		}
		let adAccounts = accountIds.split(",");
		let sortBy = req.body.sortBy && req.body.sortBy != "" ? req.body.sortBy : "media_buyer";
		let sortOrder = req.body.sortOrder && req.body.sortOrder != "" ? req.body.sortOrder : 1;
		let sortObject = {};
		let searchTerm = req.body.searchTerm ?? "";
		sortObject[sortBy] = sortOrder;
		let limit = req.body.limit && req.body.limit != "" ? req.body.limit : 20;
		let page = req.body.page && req.body.page != "" ? req.body.page : 1;

		let firstMatch = {};
		firstMatch.adDate = {
			$gte: startDateTimeStamp,
			$lte: endDateTimeStamp,
		};
		// firstMatch['$and'] = [{ adDate: { $gte: startDateTimeStamp }, adHour: { $gte: startHour } }, { adDate: { $lte: endDateTimeStamp }, adHour: { $lte: endHour } }];
		let mediaBuyerMatch = {};
		let mediaBuyers;
		searchTerm != ""
			? ((mediaBuyerMatch["$or"] = [
				{ userName: { $regex: searchTerm, $options: "i" } },
				{ firstName: { $regex: searchTerm, $options: "i" } },
				{ lastName: { $regex: searchTerm, $options: "i" } },
				{ email: { $regex: searchTerm, $options: "i" } },
			]),
				(mediaBuyers = await adx_user.find(mediaBuyerMatch).lean()))
			: (mediaBuyers = await adx_user.find().lean());
		let skip = limit * (page - 1);
		filters.length == 0
			? ((filterobject = {}), (filterobject.filterName = "fbClicks"), (filterobject.filterOperator = "greater_than"), (filterobject.filterValue = -1), (filters = []), filters.push(filterobject))
			: (t = 0);

		let response = {};
		response.statusCode = 200;
		response.status = true;
		response.success = true;
		let responseData = {};
		await saveFilters(filters, userId, filterGroupName);
		let aggregatedData = await getGroupedData(mediaBuyers, firstMatch, sortObject, skip, limit);
		let totalCost = 0;
		let totalSpend = 0;
		let totalFbLinkClicks = 0;
		let totalVoluumConversions = 0;
		let totalRevenue = 0;
		let totalVoluumClicks = 0;
		let finalMetrics = aggregatedData.filter((objectKey) => {
			let returnValue = true;
			if (filters.length > 0) {
				for (var filter of filters) {
					switch (filter.filterOperator) {
						case "greater_than":
							if (parseFloat(objectKey[filter.filterName]) > parseFloat(filter.filterValue)) {
								totalCost += parseFloat(objectKey["fbSpendWithTaxes"]);
								totalSpend += parseFloat(objectKey["fbSpend"]);
								totalFbLinkClicks += parseFloat(objectKey["fbLinkClicks"]);
								totalVoluumConversions += parseFloat(objectKey["voluumConversions"]);
								totalRevenue += parseFloat(objectKey["volRevenue"]);
								totalVoluumClicks += parseFloat(objectKey["voluumClicks"]);
							} else {
								returnValue = false;
								return returnValue;
							}
							break;

						case "less_than":
							if (parseFloat(objectKey[filter.filterName]) < parseFloat(filter.filterValue)) {
								totalCost += parseFloat(objectKey["fbSpendWithTaxes"]);
								totalSpend += parseFloat(objectKey["fbSpend"]);
								totalFbLinkClicks += parseFloat(objectKey["fbLinkClicks"]);
								totalVoluumConversions += parseFloat(objectKey["voluumConversions"]);
								totalRevenue += parseFloat(objectKey["volRevenue"]);
								totalVoluumClicks += parseFloat(objectKey["voluumClicks"]);
							} else {
								returnValue = false;
								return returnValue;
							}
							break;

						case "equal_to":
							if (parseFloat(objectKey[filter.filterName]) == parseFloat(filter.filterValue)) {
								totalCost += parseFloat(objectKey["fbSpendWithTaxes"]);
								totalSpend += parseFloat(objectKey["fbSpend"]);
								totalFbLinkClicks += parseFloat(objectKey["fbLinkClicks"]);
								totalVoluumConversions += parseFloat(objectKey["voluumConversions"]);
								totalRevenue += parseFloat(objectKey["volRevenue"]);
								totalVoluumClicks += parseFloat(objectKey["voluumClicks"]);
							} else {
								returnValue = false;
								return returnValue;
							}

							break;

						case "not_equal_to":
							if (parseFloat(objectKey[filter.filterName]) != parseFloat(filter.filterValue)) {
								totalCost += parseFloat(objectKey["fbSpendWithTaxes"]);
								totalSpend += parseFloat(objectKey["fbSpend"]);
								totalFbLinkClicks += parseFloat(objectKey["fbLinkClicks"]);
								totalVoluumConversions += parseFloat(objectKey["voluumConversions"]);
								totalRevenue += parseFloat(objectKey["volRevenue"]);
								totalVoluumClicks += parseFloat(objectKey["voluumClicks"]);
							} else {
								returnValue = false;
								return returnValue;
							}
							break;
					}
				}
			} else {
				totalCost += parseFloat(objectKey["fbSpendWithTaxes"]);
				totalSpend += parseFloat(objectKey["fbSpend"]);
				totalFbLinkClicks += parseFloat(objectKey["fbLinkClicks"]);
				totalVoluumConversions += parseFloat(objectKey["voluumConversions"]);
				totalRevenue += parseFloat(objectKey["volRevenue"]);
				totalVoluumClicks += parseFloat(objectKey["voluumClicks"]);
			}

			if (returnValue == false) {
				return 2 > 4;
			} else {
				return 2 < 4;
			}
		});
		let sortedArray = await getSortedArray(sortOrder, sortBy, finalMetrics);
		let totalNet = totalRevenue - totalCost;
		let roi = totalCost != 0 ? (totalNet / totalCost) * 100 : 0;
		let avgCpc = totalFbLinkClicks != 0 ? parseFloat(totalSpend) / parseFloat(totalFbLinkClicks) : 0;
		let avgCpa = totalVoluumConversions != 0 ? parseFloat(totalCost) / parseFloat(totalVoluumConversions) : 0;
		let avgEpc = totalVoluumClicks != 0 ? parseFloat(totalRevenue) / parseFloat(totalVoluumClicks) : 0;
		userId != "none" ? (userFilters = await getFilter(userId)) : (userFilters = []);
		let totalCount = sortedArray.length;
		let totalPage = Math.ceil(totalCount / limit);
		if (page > totalPage) {
			page = totalPage;
		}
		let lists = sortedArray.slice(page * limit - limit, page * limit);
		typeof userFilters !== "null" && userFilters !== null ? (t = 0) : (userFilters = []);
		responseData.totalCost = typeof totalCost !== "null" && totalCost !== null ? totalCost : 0;
		responseData.avgCpc = typeof avgCpc !== "null" && avgCpc !== null ? avgCpc : 0;
		responseData.totalRevenue = typeof totalRevenue !== "null" && totalRevenue !== null ? totalRevenue : 0;
		responseData.avgCpa = typeof avgCpa !== "null" && avgCpa !== null ? avgCpa : 0;
		responseData.totalNet = typeof totalNet !== "null" && totalNet !== null ? totalNet : 0;
		responseData.avgEpc = typeof avgEpc !== "null" && avgEpc !== null ? avgEpc : 0;
		responseData.totalRoi = typeof roi !== "null" && roi !== null ? roi : 0;
		responseData.roi = typeof roi !== "null" && roi !== null ? roi : 0;
		responseData.totalPage = totalPage;
		responseData.limit = limit;
		responseData.currentPage = page;
		responseData.totalRecords = totalCount;
		responseData.userFilters = userFilters;
		responseData.metrics = lists;
		response.message = "Media Buyers Metrices Loaded successfully";
		response.data = responseData;

		return res.json(response);
	} catch (e) {
		console.trace(e);
	}
};
module.exports = {
	getMediaBuyerMetrics,
};
