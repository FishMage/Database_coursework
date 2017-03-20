/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"

//#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string & relationName,
			std::string & outIndexName,
			BufMgr *bufMgrIn,
			const int attrByteOffset,
			const Datatype attrType)
	{
		std::ostringstream idxStr;
		idxStr << relationName << '.' << attrByteOffset;
		outIndexName = idxStr.str();// name of the index file
		this->bufMgr = bufMgrIn;
		this->attrByteOffset = attrByteOffset;
		this->attributeType = attrType;
		scanExecuting = false;

		leafOccupancy = 0 ;
		nodeOccupancy = 0;
		//headerPageNum = 1;

		IndexMetaInfo* metadata;
		Page* page;
		Page* metapage;
		Page* rootpage;
		// try create a file and check if it exists
		try {

			//metapage
			file = new BlobFile(outIndexName, true);
			bufMgrIn->allocPage(file, headerPageNum, metapage);
			metadata = (IndexMetaInfo*) metapage;
			metadata->attrByteOffset = attrByteOffset;
			metadata->attrType = attrType;
			strcpy(metadata->relationName, relationName.c_str());

			//root page
			bufMgrIn->allocPage(file, rootPageNum, rootpage);
			metadata->rootPageNo = rootPageNum;
			bufMgr->unPinPage(file, headerPageNum, true); // surround by try block? 

			//initialize root
			NonLeafNodeInt *root = (NonLeafNodeInt *) rootpage;
			for(int i = 0; i<INTARRAYNONLEAFSIZE + 1; i++) 
				root->pageNoArray[i] = 0;
			for(int i = 0; i<INTARRAYNONLEAFSIZE; i++) 
				root->keyArray[i] = 0;
			root->level = 0;
			//INTARRAYLEAFSIZE, INTARRAYNONLEAFSIZE//
			std::cout<<"INTARRAYLEAFSIZE:"<<INTARRAYLEAFSIZE<<std::endl;
			std::cout<<"INTARRAYNONLEAFSIZE:"<<INTARRAYNONLEAFSIZE<<std::endl;


			bufMgrIn->unPinPage(file, rootPageNum, true);

			//scan records
			try{		
				FileScan fscan(relationName, bufMgr);
				RecordId scanRid;
				while(1) {	
					fscan.scanNext(scanRid);
					insertEntry((int *) fscan.getRecord().c_str() + attrByteOffset, scanRid);
				}
			} catch (EndOfFileException e){ }
		}
		// file exists, just open it
		catch (FileExistsException e){
			this->file = new BlobFile(outIndexName, false);
			bufMgrIn->readPage(file, headerPageNum, page);
			metadata = (IndexMetaInfo*) page;
			if(metadata->attrType != attrType || metadata->attrByteOffset != attrByteOffset 
					|| strcmp(metadata->relationName, relationName.c_str()) != 0){
				throw BadIndexInfoException("MetaData does not match");
			}
			//Metadata matches
			this->rootPageNum = metadata->rootPageNo;
			bufMgrIn->unPinPage(file, headerPageNum, false);
		}
		//Catch exception from unpinning page
		catch (PageNotPinnedException e) {
		}

	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		if(scanExecuting == true) {
			try {
				endScan();
			} 
			catch(const ScanNotInitializedException &e) { }
		}
		bufMgr->flushFile(file);
		file->~File();
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::createNewRoot
	// create a new root node when necessary , set NewRoot Level to 0
	// @Parameters: PageId newChildId, int newKey
	// 
	// -----------------------------------------------------------------------------
	void BTreeIndex::createNewRoot(PageId newChildId, int newKey){
		//Alloc space for the new root
		PageId newRootId;
		Page *page;
		Page *oldRootPage;
		bufMgr->allocPage(file, newRootId, page);
		bufMgr->readPage(file, rootPageNum, oldRootPage);
		NonLeafNodeInt *oldRoot = (NonLeafNodeInt *) oldRootPage;
		NonLeafNodeInt* newRoot = (NonLeafNodeInt *) page;
		
		//Test
		newRoot->level = oldRoot->level++ ;
		//CLEAR the PageNoArray[]
		for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++){
			newRoot->pageNoArray[i] = 0;
		}
		//Setup newRoot
		newRoot->pageNoArray[0] = rootPageNum;
		newRoot->pageNoArray[1] = newChildId;
		rootPageNum = newRootId;

		//Release both pages
		try{
			bufMgr->unPinPage(file, newRootId, true);
		}
		catch (PageNotPinnedException e) {}	
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::checkFull
	// check whether the current node is full 
	// @Parameters:
	// @return: True if full
	// 
	// -----------------------------------------------------------------------------
	int BTreeIndex::leafCheckFull(LeafNodeInt *node)
	{
		for( int i = 0; i < INTARRAYLEAFSIZE; i++){
			//std::cout<<"LEAF_CHECK_FULL: "<<  node->ridArray[i].page_number<<std::endl;
			if(node->ridArray[i].page_number == 0){
				return i;
				
			}
		}
		return INTARRAYLEAFSIZE;
	}

	int BTreeIndex:: nonLeafCheckFull(NonLeafNodeInt *node){
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++){
			//std::cout<<"NON_LEAF_CHECK_FULL: "<< node->pageNoArray[i + 1]<<std::endl;
//			if(node->pageNoArray[0] == 0)
//				return 0;
			if(node->pageNoArray[i + 1] == 0){
				return i;
				
			}
		}
		return INTARRAYNONLEAFSIZE;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::addLeaf
	// 
	// 
	// -----------------------------------------------------------------------------
	void BTreeIndex::initLeaf(RIDKeyPair<int> pair,NonLeafNodeInt* node)
	{
		PageId newPageId;
		Page* newPage;

		//Alloc new memory space for the new leaf
		bufMgr->allocPage(file, newPageId, newPage);
		LeafNodeInt *leaf = (LeafNodeInt *) newPage;

		//Init values
		for(int i  = 0; i < INTARRAYLEAFSIZE; i++){
			leaf->ridArray[i].page_number = 0;
			if(i == 0){
				leaf->keyArray[i] = pair.key;
				leaf->ridArray[i] = pair.rid;
			}
			//Set up values in the parent node
			node->pageNoArray[0] = newPageId;
			//Set sigbling = 0 since it is the first leafAdded to the B+tree;
			leaf->rightSibPageNo = 0;
			//releas the temp page
			try{
				bufMgr->unPinPage(file, newPageId, true);
			}
			catch (PageNotPinnedException e) {}		
		}
//		for(int i  = 0; i < INTARRAYLEAFSIZE; i++){
//			std::cout<<"InitLeaf: "<<leaf->ridArray[i].page_number<<std::endl;
//		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::split
	// split the node when necessary
	// 
	// -----------------------------------------------------------------------------
	void BTreeIndex::leafSplit(RIDKeyPair<int> pair, 
			LeafNodeInt* node,
			int newPushedUpKey,	 //return value for adding new key toparent KeyArraykey
			PageId newSplitPageId,//return value for adding new page parent PageNoArray
			int targetPos	 //Posiion in the current that will add the new key
			){
		//Establish a array to sort all keys
		int sortedKey[INTARRAYLEAFSIZE + 1];
		RecordId sortedRid[INTARRAYLEAFSIZE +1];

		Page* newLeafPage;
		bufMgr->allocPage(file, newSplitPageId, newLeafPage);
		LeafNodeInt *newLeaf = (LeafNodeInt *) newLeafPage;
	//TEST Befor split
	for(int i = 0; i < INTARRAYLEAFSIZE; i++){
		//	std::cout<<" OriginalRID:"<<node->ridArray[i].page_number<<std::endl;
		//	std::cout<<" OriginalKEY:"<<node->keyArray[i]<<std::endl;
			
			}

		//copy the original array
		for(int i = 1; i< INTARRAYLEAFSIZE+1; i++){
			sortedRid[i] = node->ridArray[i - 1];
			sortedKey[i] = node->keyArray[i - 1];
			//Initialize the both leaf
			node->ridArray[i - 1].page_number = 0;
			newLeaf->ridArray[i - 1].page_number = 0;		
		}
		//Add new key to the array and sort
		for(int i = 0; i< targetPos; i++){
			//LEFT SHIFT
			sortedRid[i] = sortedRid[i + 1];
			sortedKey[i] = sortedKey[i + 1];
		}
		//IF targetPos reaches, ADD 
		sortedRid[targetPos] = pair.rid;
		sortedKey[targetPos] = pair.key;

		for(int i = 0; i< INTARRAYLEAFSIZE; i++){
			node->ridArray[i].page_number = 0;
			newLeaf->ridArray[i].page_number = 0;
			}

		//Finish sort, redistrubute keys and rids
		int midVal = (INTARRAYLEAFSIZE + 1)/2;
		for(int i = 0; i < INTARRAYLEAFSIZE + 1; i++){
			if(i < midVal){
				node->ridArray[i] = sortedRid[i];
				node->keyArray[i] = sortedKey[i]; 
//				std::cout<<"inserting Key: "<<node->keyArray[i]<<std::endl;
			}
			else if(i >= midVal){
				newLeaf->ridArray[i - midVal] = sortedRid[i];
				newLeaf->keyArray[i - midVal] = sortedKey[i];		
//				std::cout<<"inserting Key: "<<sortedKey[i]<<std::endl;
			}
		}

		//Setup return Value	
		newPushedUpKey = newLeaf->keyArray[0];		

		//Setup sibling (insert)
		newLeaf->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = newSplitPageId;
		//Release the current page
		for(int i = 0; i < INTARRAYLEAFSIZE; i++){
//			std::cout<<" Old:"<<node->ridArray[i].page_number<<std::endl;
//			std::cout<<" key:"<<node->keyArray[i]<<std::endl;
//			std::cout<<" New:"<<newLeaf->ridArray[i].page_number<<std::endl;
			}
		std::cout<<"NewLeaf: "<<newSplitPageId<<std::endl;
		try{
			bufMgr->unPinPage(file, newSplitPageId, true);
		}
		catch (PageNotPinnedException e) {}		


	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::split
	// split the node when necessary
	// 
	// -----------------------------------------------------------------------------
	void BTreeIndex::nonLeafSplit(
			NonLeafNodeInt* node, //Current node
			int newPushedUpKey,	 //return value for adding new key toparent KeyArraykey
			PageId newSplitPageId,//return value for adding new page parent PageNoArray
			int targetPos,	//potential index to add the new key
			int childKey,	//Parameter that contain the new created key in the child level
			PageId childPageId	//parameter indicate the new created node in the child level
			){
		std::cout<<"split Non-Leaf"<<std::endl;
		//Establish a array to sort all keys
		int sortedKey[INTARRAYNONLEAFSIZE + 1];
		PageId sortedPageNo[INTARRAYNONLEAFSIZE + 2];

		//allocate new memory space for the new Nonleafnode
		Page* newNonLeafPage;
		bufMgr->allocPage(file, newSplitPageId, newNonLeafPage);
		NonLeafNodeInt *newNonLeaf = (NonLeafNodeInt *) newNonLeafPage;	

		//make a copy of the original arrays
		for(int i = 1; i< INTARRAYNONLEAFSIZE+1; i++){
			sortedPageNo[i] = node->pageNoArray[i - 1];
			sortedKey[i] = node->keyArray[i - 1];
		}
		sortedPageNo[INTARRAYNONLEAFSIZE+1] = node->pageNoArray[INTARRAYNONLEAFSIZE]; //???
		//Init both nonleaves
		for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++){
			node->pageNoArray[i] = 0;
			newNonLeaf->pageNoArray[i] = 0;
		}
		//SORT:
		//LEFT SHIFT AND INSERT
		for(int i = 0; i <= targetPos; i++){
			sortedKey[i] = sortedKey[i + 1];
			sortedPageNo[i] = sortedPageNo[i + 1];
		}
		//IF targetPos reaches, ADD 
		sortedPageNo[targetPos] = childPageId ;
		sortedKey[targetPos ] = childKey;

		//Redistribution
		int midVal = (INTARRAYNONLEAFSIZE + 2) / 2;//4
		for(int i = 0; i < INTARRAYNONLEAFSIZE + 2/*8*/; i++){
			if(i < midVal){    // 0-3
				node->keyArray[i] = sortedKey[i];
				node->pageNoArray[i] = sortedPageNo[i];
			}
			else if(i == midVal){ //4
				node->pageNoArray[midVal] = sortedPageNo[midVal];
			}
			else if(i > midVal){// 4-7
				newNonLeaf->keyArray[i - midVal - 1] = sortedKey[i - 1]; //???
				newNonLeaf->pageNoArray[i - midVal - 1] = sortedPageNo[i -1];
			}

		}
		//Insert the last value in the PageNoArray
		newNonLeaf->pageNoArray[midVal - 1] = sortedPageNo[INTARRAYNONLEAFSIZE+1];

		//Set up value for the new nonLeaf
		newPushedUpKey = newNonLeaf->keyArray[0];
		newNonLeaf->level = node->level;
		//Release the page
		try{
			bufMgr->unPinPage(file, newSplitPageId, true);
		}
		catch (PageNotPinnedException e) {}	

	}
	// -----------------------------------------------------------------------------
	// BTreeIndex::findAndInsert
	// split the node when necessary
	// parameters: pair, pageId, newChildPageId, isLeaf, 
	// USE: INTARRAYLEAFSIZE, INTARRAYNONLEAFSIZE
	// -----------------------------------------------------------------------------
	void BTreeIndex::findAndInsert(RIDKeyPair<int> pair,
			PageId pageId, 
			bool isLeaf,
			int newPushedUpKey,	//Use in split function
			PageId newSplitPageId	//Use in split function
			){
		//Handle non-leaf nodes
		if(!isLeaf){

			//Get info from the given pageId and store in a temp page
			Page* page;
			bufMgr->readPage(file, pageId, page);
			NonLeafNodeInt *node = (NonLeafNodeInt *) page;
//			std::cout<<"reading Non-Leaf pageNo: " << pageId<<"RootPage: "<<rootPageNum<<std::endl;
			//Find Appropriate position to insert
			int targetPos = 0;
//			for(targetPos = 0; targetPos < INTARRAYNONLEAFSIZE; targetPos++){
//				if((pair.key < node->keyArray[targetPos] || node->pageNoArray[targetPos + 1] == 0)
//					break;
//			}
//				std::cout<<"Comparing Key "<<pair.key<<" AND "<<node->keyArray[targetPos]<<std::endl;
			while(pair.key >= node->keyArray[targetPos] && node->pageNoArray[targetPos + 1]!= 0 && targetPos < INTARRAYNONLEAFSIZE ){
				std::cout<<"Comparing Key "<<pair.key<<" >= "<<node->keyArray[targetPos]<<std::endl;
				targetPos++;
			}

//			std::cout<<"targetPOs:"<<targetPos<<std::endl;
//			std::cout<<"Key:"<<node->keyArray[targetPos]<<std::endl;
			// init with a new leaf
			if(node->pageNoArray[targetPos] == 0){
				std::cout<<"Init newLeaf"<<std::endl;
				initLeaf(pair,node);	
				return;
			}

			// unpin the temp page
			try{
				bufMgr->unPinPage(file, pageId, true);		
			}catch (PageNotPinnedException e) {}
			//FindAndInsert in the child node
			PageId newChildPageId = 0;	//
			int newChildKey = 0;//store the new pushed up key value if split happens
			findAndInsert(pair, node->pageNoArray[targetPos], node->level == 0, newChildKey, newChildPageId);
//			std::cout<<"findAndInsert: targetPos: "<<targetPos<<std::endl;

			//Check whether the array is full
			if(newChildPageId != 0){
				//get the page again for spliting
				bufMgr->readPage(file, pageId, page);
				NonLeafNodeInt *node = (NonLeafNodeInt *) page;
				//check full
				int currNodeSize = nonLeafCheckFull(node);
			//	std::cout<<"CurrNodeSize: "<<currNodeSize<<std::endl;
				//FULL, split
				if(currNodeSize == INTARRAYNONLEAFSIZE){
					nonLeafSplit(node,newPushedUpKey,newSplitPageId,targetPos,newChildKey,newChildPageId);
				}
				//Not full, shift
				else{
					for(int j = currNodeSize; j > targetPos; j--){
						node->keyArray[j] = node->keyArray[j -1]; //KeyArray
						node->pageNoArray[j + 1] = node->pageNoArray[j];//PageId
					}
					//Insert new pageID
					node->keyArray[targetPos] = newChildKey;
					node->pageNoArray[targetPos + 1] = newChildPageId;
					std::cout<<"new page added to"<<targetPos + 1<<" pageId = "<<newChildPageId;
				}
			}

			//Finish insert, release the temp page
			try{
				bufMgr->unPinPage(file, pageId, true);		
			}catch (PageNotPinnedException e) {}
		}

		//Handle LeafNodes
		else{
			Page* page;
			bufMgr->readPage(file, pageId, page);
			LeafNodeInt *node = (LeafNodeInt *) page;

			//Find Appropriate position to insert
			int targetPos;
			for(targetPos = 0; targetPos < INTARRAYLEAFSIZE; targetPos++)
				if(pair.key <= node->keyArray[targetPos] 
						|| node->ridArray[targetPos].page_number ==0 )
					break;

			//Find last Rid Entry
			int currNodeSize = leafCheckFull(node);
			if(currNodeSize == -1) std::cout<<"ERROR in LEAF_CHECK_FULL"<<std::endl;
//			std::cout<<"var CurrNodeSize: "<<currNodeSize<<std::endl;

			//full, split required
			if(currNodeSize == INTARRAYLEAFSIZE){
				std::cout<<"Split PageNo: "<<pageId<<std::endl;
				leafSplit(pair, node,newPushedUpKey,newSplitPageId,targetPos);
			}
			//Not full, insert
			else{
				//Right shift all the element on the right of the targetPos in both arrays  
				for(int j = currNodeSize; j > targetPos; j--){
					node->keyArray[j] = node->keyArray[j -1]; //KeyArray
					node->ridArray[j] = node->ridArray[j -1];//PageId
				}
				//Insert in target position
				node->keyArray[targetPos] = pair.key;
				node->ridArray[targetPos] = pair.rid;
			}
			try{
				bufMgr->unPinPage(file, pageId, true);		
			}catch (PageNotPinnedException e) {}
		}
	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// Helper: insertRec()
	// 	    split(,isLeaf)
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
	{
		//SCHEN-Test
		//std::cout<<"INSERT"<<std::endl;

		//Setup the RidKeyPair
		RIDKeyPair<int> pair;
		pair.set(rid, *((int *) key));
//		std::cout<<"INSERT: Key--"<<pair.key<<" rid-- "<<pair.rid<<std::endl;
		PageId newPageId = 0;	//new PageId if split happens
		int newChildKey = 0; 	//new Key pushed up if split happens
		//Begin insert
		findAndInsert(pair,rootPageNum,false,newChildKey,newPageId);
		//Handle newroot split
		if(newPageId != 0)
			createNewRoot(newPageId,newChildKey);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------

	const void BTreeIndex::startScan(const void* lowValParm,
			const Operator lowOpParm,
			const void* highValParm,
			const Operator highOpParm)
	{
		//Validate scan
		if((lowOpParm != GT && lowOpParm != GTE)||(highOpParm != LT && highOpParm != LTE))					
			throw BadOpcodesException();
				//Copy the value
		lowValInt = *(int *)lowValParm;
		highValInt = *(int *)highValParm;
		lowOp = lowOpParm;
		highOp = highOpParm;
		scanExecuting = true;
		if(lowValInt > highValInt)
			throw BadScanrangeException();

		std::cout<<"lowValParm: "<<lowValInt<<"  highValParm: "<<highValInt<<"  lowOpParm: "<<lowOp<<"  highOpParm: "<<highOp<<std::endl;
		NonLeafNodeInt * currNode = findParentOfLeaf(lowValInt, rootPageNum);	
		//currentPageNum now pointing to the parent node of the target leaf
		//Get info of target leaf

		int idx = 0;
		while(idx < INTARRAYNONLEAFSIZE && currNode->keyArray[idx] <= lowValInt && currNode->pageNoArray[idx + 1]!=0){
			idx++;
		}
		try{
			bufMgr->unPinPage(file,currentPageNum,false);
		}catch (PageNotPinnedException e) {}

		//Set page
		currentPageNum = currNode->pageNoArray[idx];
		bufMgr->readPage(file,currentPageNum, currentPageData);
		nextEntry = 0;	
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::findLeaf
	// tracedown the b+tree to find the target leaf that contains the appropriate range
	// -----------------------------------------------------------------------------
	NonLeafNodeInt* BTreeIndex::findParentOfLeaf(int lowVal,PageId currPage){
		//Read info of the currentPae
		currentPageNum = currPage;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		NonLeafNodeInt* currNode = (NonLeafNodeInt*) currentPageData;
		std::cout<<"currNode-Level = "<<currNode->level<<std::endl;
		//Handle the data
		if(currNode->level == 0)
		{
			//SCHEN-Test
//			std::cout<<"Target leafNode's parent found: currentPageNum = "<<currentPageNum<<""
			return currNode;
		}
		//Paged = childPage;
		int idx = 0;
		while(idx < INTARRAYNONLEAFSIZE && currNode->keyArray[idx] <= lowVal && currNode->pageNoArray[idx + 1]!=0){
			idx++;
		}
		PageId childPage = currNode->pageNoArray[idx];
		try{
			bufMgr->unPinPage(file,currPage,false);
		}catch (PageNotPinnedException e) {}

		findParentOfLeaf(lowVal,childPage); 
		return NULL;	
	}
	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	const void BTreeIndex::scanNext(RecordId& outRid) 
	{
		if(!scanExecuting)	
			throw ScanNotInitializedException();
		while(1){
			if (checkJumpPage()) continue; 
			if(adjustEntry()) continue;
			if(checkReachHigh()) continue;
			//Return the rid	 
			LeafNodeInt* currNode = (LeafNodeInt *)currentPageData; 	
			outRid = currNode->ridArray[nextEntry];
			//std::cout<<"KEY: "<<currNode->keyArray[nextEntry]<<std::endl;
			nextEntry++;
			return;
		}
	}

	bool BTreeIndex::checkJumpPage(){
		LeafNodeInt* currNode = (LeafNodeInt *)currentPageData; 	
//		std::cout<<"CHECKING Jump...Next Entry:"<<nextEntry<<std::endl;
//		std::cout<<"PageNO: "<< currNode->ridArray[nextEntry+1].page_number<<std::endl;
		//END OF ARRAY, jump to right sibling
		if(currNode->ridArray[nextEntry].page_number == 0||nextEntry == INTARRAYLEAFSIZE ){
			
			std::cout<<"SHOULDJUMP"<<std::endl;
			try{
				bufMgr->unPinPage(file, currentPageNum, false);
			}catch (PageNotPinnedException e){ 
			}
			//if exhauested all possible value
			if(currNode->rightSibPageNo == 0)
			{
				throw IndexScanCompletedException();
			}
			std::cout<<"RIGHTSIBNUM: "<<currNode->rightSibPageNo<<std::endl;
			currentPageNum = currNode->rightSibPageNo;
			
			bufMgr->readPage(file,currentPageNum,currentPageData);
			nextEntry = 0;
			return true;

		}
		return false;
	}
	bool BTreeIndex::checkReachHigh(){
		LeafNodeInt* currNode = (LeafNodeInt *)currentPageData; 	
		if(highOp == LT && highValInt <= currNode->keyArray[nextEntry])
			throw IndexScanCompletedException();
		if(highOp == LTE && highValInt < currNode->keyArray[nextEntry])
			throw IndexScanCompletedException();
		return false;
	}

	bool BTreeIndex::adjustEntry(){
		LeafNodeInt* currNode = (LeafNodeInt *)currentPageData; 
		//Skip records that does not need to be return 
		if(lowOp == GT && lowValInt >= currNode->keyArray[nextEntry]){
			nextEntry++;
			return true;
		}
		if(lowOp == GTE && lowValInt > currNode->keyArray[nextEntry]){
			nextEntry++;
			return true;
		}
		return false;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	const void BTreeIndex::endScan() 
	{
		if(!scanExecuting)
			throw ScanNotInitializedException();
		scanExecuting = false;
		try {
			bufMgr->unPinPage(file, currentPageNum, false);
		} catch (PageNotPinnedException e) {} 
	}
}
