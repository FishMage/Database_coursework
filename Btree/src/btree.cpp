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
		headerPageNum = 1;


		// try create a file and check if it exists
		try {
			IndexMetaInfo* metadata;
			//Page* page;
			Page* metapage;
			Page* rootpage;

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
			std::cout<<"RootPageNo = "<<rootPageNum<<"  headerPageNum = "<<headerPageNum<<std::endl;
			try{
				bufMgrIn->unPinPage(file, headerPageNum, true);
			} catch (PageNotPinnedException e) {
			}

			//initialize root
			NonLeafNodeInt *root = (NonLeafNodeInt *) rootpage;
			for(int i = 0; i<INTARRAYNONLEAFSIZE + 1; i++) 
				root->pageNoArray[i] = 0;
			for(int i = 0; i<INTARRAYNONLEAFSIZE; i++) 
				root->keyArray[i] = 0;
			root->level = 1;
			//INTARRAYLEAFSIZE, INTARRAYNONLEAFSIZE//
			std::cout<<"INTARRAYLEAFSIZE:"<<INTARRAYLEAFSIZE<<std::endl;
			std::cout<<"INTARRAYNONLEAFSIZE:"<<INTARRAYNONLEAFSIZE<<std::endl;

			try{
				bufMgrIn->unPinPage(file, rootPageNum, true);
			}catch (PageNotPinnedException e) {}
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
			Page* metapage;
			//headerPageNum = 1;
			bufMgrIn->readPage(file, headerPageNum, metapage);
			IndexMetaInfo* metadata = (IndexMetaInfo*) metapage;
			if(metadata->attrType != attrType ||strcmp(metadata->relationName, relationName.c_str()) != 0
					|| metadata->attrByteOffset != attrByteOffset ){
				try {
					bufMgrIn->unPinPage(file, headerPageNum, false);
				} catch (PageNotPinnedException e ){}
				throw BadIndexInfoException("ERROR: MetaData does not match");
			}
			//Metadata matches
			this->rootPageNum = metadata->rootPageNo;

			try {
				bufMgrIn->unPinPage(file, headerPageNum, false);
			} catch (PageNotPinnedException e ){}
		}
		catch (EndOfFileException e){}


	}


	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{

		scanExecuting = false;
		try {
			bufMgr->unPinPage(file, currentPageNum, false);
		} 
		catch (HashNotFoundException e) {} 
		catch (PageNotPinnedException e) {}

		//bufMgr->printSelf();
		bufMgr->flushFile(file);
		file->~File();

	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::findAndInsert
	// Recursive method to traverse down the B+tree to insert the given <key, rid> pair
	// check whether the current leaf node is full 
	// @return: the index of avaliable spot 
	// -----------------------------------------------------------------------------
	void BTreeIndex::findAndInsert(RIDKeyPair<int> pair,
			PageId pageId, 
			bool isLeaf,
			int & newPushedUpKey,	//Return value used in split function
			PageId& newSplitPageId	//Return value used in split function
			){
		//Handle non-leaf nodes
		if(!isLeaf){

			//Get info from the given pageId and store in a temp page
			Page* page;
			bufMgr->readPage(file, pageId, page);
			NonLeafNodeInt *node = (NonLeafNodeInt *) page;

			//Find Appropriate position to insert
			int targetPos = 0;

			for(targetPos = 0; targetPos < INTARRAYNONLEAFSIZE; targetPos++)
				if(pair.key < node->keyArray[targetPos] || node->pageNoArray[targetPos + 1] == 0)
					break;


			// both PageNoArray Entry on the left and right of the 
			// target key are empty --> The node does not have any valid key.
			// So initLeaf() will create a new leaf in the current B+ tree index
			if(node->pageNoArray[targetPos] == 0){
				std::cout<<"Init newLeaf"<<std::endl;
				initLeaf(pair,node, pageId);	
				return;
			}

			// unpin the temp page
			try{
				bufMgr->unPinPage(file, pageId, false);		
			}catch (PageNotPinnedException e) {}
			//FindAndInsert in the child node
			PageId newChildPageId = 0;	//change if child get split
			int newChildKey;		//store the new pushed up key value if split happens
			findAndInsert(pair, node->pageNoArray[targetPos], node->level == 1, newChildKey, newChildPageId);


			//Check whether the array is full
			if(newChildPageId != 0){			

				//get the page again for spliting
				bufMgr->readPage(file, pageId, page);
				NonLeafNodeInt *node = (NonLeafNodeInt *) page;

				//check full
				int currNodeSize = nonLeafCheckFull(node);
				if(currNodeSize > INTARRAYNONLEAFSIZE) std::cout<<"ERROR in LEAF_CHECK_FULL"<<std::endl;

				//IF FULL, split
				if(currNodeSize == INTARRAYNONLEAFSIZE){
					nonLeafSplit(node, newPushedUpKey, newSplitPageId, targetPos, newChildKey, newChildPageId);
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
			if(currNodeSize > INTARRAYLEAFSIZE) std::cout<<"ERROR in LEAF_CHECK_FULL"<<std::endl;

			//full, split required
			if(currNodeSize == INTARRAYLEAFSIZE)
				leafSplit(pair, node, newPushedUpKey, newSplitPageId, targetPos);

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
	// Helper: findAndInsert()
	// 	   createNewRoot()
	// -----------------------------------------------------------------------------

	const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
	{
		//Setup the RidKeyPair
		RIDKeyPair<int> pair;
		pair.set(rid, *((int *) key));

		PageId newPageId = 0;	//stores new PageId if split happens
		int newChildKey; 		//stores new Key pushed up if split happens

		//Begin insert by calling the recursive insert:
		findAndInsert(pair,rootPageNum,false, newChildKey, newPageId);

		//Handle newroot split
		if(newPageId != 0)
			createNewRoot(newPageId,newChildKey);
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::checkFull
	// check whether the current node is full 
	// @param newChildId: the PageId of the newly created child.
	// @param newKey: the push up Key of the child, which will be the first key
	// 	  of the new Root
	// @return: the index of avaliable spot 
	// -----------------------------------------------------------------------------
	void BTreeIndex::createNewRoot(PageId newChildId, int& newKey){
		std::cout<<"New Root Created"<<std::endl;

		//Alloc space for the new root
		PageId newRootId, oldRootPageNum;
		Page *page;
		Page *oldRootPage;
		bufMgr->allocPage(file, newRootId, page);
		bufMgr->readPage(file, rootPageNum, oldRootPage);
		NonLeafNodeInt *oldRoot = (NonLeafNodeInt *) oldRootPage;
		NonLeafNodeInt* newRoot = (NonLeafNodeInt *) page;

		// the parent of leaves will be level 1, upper level will be 2,3,4...
		newRoot->level = oldRoot->level + 1 ;

		//CLEAR the PageNoArray[]
		for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++){
			newRoot->pageNoArray[i] = 0;
		}
		//Setup newRoot
		newRoot->keyArray[0] = newKey;
		newRoot->pageNoArray[0] = rootPageNum;
		newRoot->pageNoArray[1] = newChildId;
		oldRootPageNum = rootPageNum;
		rootPageNum = newRootId;

		//Release both pages
		try{
			bufMgr->unPinPage(file, newRootId, true);
			bufMgr->unPinPage(file, oldRootPageNum, true);
		}
		catch (PageNotPinnedException e) {}	
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::checkFull
	// check whether the current leaf node is full 
	// @return: the index of avaliable spot 
	// -----------------------------------------------------------------------------
	int BTreeIndex::leafCheckFull(LeafNodeInt *node)
	{
		for( int i = 0; i < INTARRAYLEAFSIZE; i++){
			if(node->ridArray[i].page_number == 0){
				return i;
			}
		}
		return INTARRAYLEAFSIZE;
	}
	// -----------------------------------------------------------------------------
	// BTreeIndex::checkFull
	// check whether the current nonLeaf node is full 
	// @return: the index of avaliable spot 
	// -----------------------------------------------------------------------------
	int BTreeIndex:: nonLeafCheckFull(NonLeafNodeInt *node){
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++){

			if(node->pageNoArray[i + 1] == 0){
				return i;

			}
		}
		return INTARRAYNONLEAFSIZE;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::initLeaf
	// Create the very first leaf in the current B+ tree
	// @param pair: <key, rid> pair to be added to the first leaf
	// @param node: parent node
	// @param pageId: the pageNo of the parent node
	// -----------------------------------------------------------------------------
	void BTreeIndex::initLeaf(RIDKeyPair<int> pair,NonLeafNodeInt* node, PageId pageId)
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
				//Parent node should also be unpinned since the recursive call will return after initLeaf()
				bufMgr->unPinPage(file, pageId, true);
			}
			catch (PageNotPinnedException e) {}		
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::leafSplit
	// Split the leafNode and return the newly created pageId and key
	// @param pair: the <key, rid> pair that will be added to the B+ tree
	// @param node: the current node that will be split
	// @param newPushedUpKey:  return value for adding new key toparent KeyArraykey
	// @param newSplitPageId:  return value for adding new page parent PageNoArray
	// @param targetPos:	   Posiion in the current that will add the new key
	// -----------------------------------------------------------------------------
	void BTreeIndex::leafSplit(RIDKeyPair<int> pair, 
			LeafNodeInt* node,
			int& newPushedUpKey,	 //return value for adding new key toparent KeyArraykey
			PageId& newSplitPageId,  //return value for adding new page parent PageNoArray
			int targetPos		 //Posiion in the current that will add the new key
			){
		//Establish a array to sort all keys
		int sortedKey[INTARRAYLEAFSIZE + 1];
		RecordId sortedRid[INTARRAYLEAFSIZE +1];

		Page* newLeafPage;
		bufMgr->allocPage(file, newSplitPageId, newLeafPage);

		LeafNodeInt *newLeaf = (LeafNodeInt *) newLeafPage;		
		//copy the original array into sorting array
		for(int i = 1; i< INTARRAYLEAFSIZE+1; i++){
			sortedRid[i] = node->ridArray[i - 1];
			sortedKey[i] = node->keyArray[i - 1];

			//Initialize the both leaf
			node->ridArray[i - 1].page_number = 0;
			newLeaf->ridArray[i - 1].page_number = 0;		
		}

		/*****SORTING ALGORITHM STARTS*****/

		//Add new key to the array and sort
		for(int i = 0; i< targetPos; i++){
			//LEFT SHIFT
			sortedRid[i] = sortedRid[i + 1];
			sortedKey[i] = sortedKey[i + 1];
		}
		//IF targetPos reaches, Insert 
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

			}
			else if(i >= midVal){
				newLeaf->ridArray[i - midVal] = sortedRid[i];
				newLeaf->keyArray[i - midVal] = sortedKey[i];		
			}
		}

		//Setup return Value	
		newPushedUpKey = newLeaf->keyArray[0];		

		//Setup sibling (insert)
		newLeaf->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = newSplitPageId;
		try{
			bufMgr->unPinPage(file, newSplitPageId, true);
		}
		catch (PageNotPinnedException e) {}		


	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::nonLeafSplit
	// Split the nonLeaf with the input of its created child's key and PageId.
	// return the newly created pageId and key
	// @param node: the current node that will be split
	// @param newPushedUpKey:  return value for adding new key toparent KeyArraykey
	// @param newSplitPageId:  return value for adding new page parent PageNoArray
	// @param targetPos:	   Posiion in the current that will add the new key
	// @param childKey:	   Parameter that contain the new created key in the child level
	// @PageId childPageId:	   parameter indicate the new created node in the child level
	// -----------------------------------------------------------------------------	
	void BTreeIndex::nonLeafSplit(
			NonLeafNodeInt* node, 	 //Current node
			int& newPushedUpKey,	 //return value for adding new key toparent KeyArraykey
			PageId& newSplitPageId,  //return value for adding new page parent PageNoArray
			int targetPos,		 //potential index to add the new key
			int& childKey,		 //Parameter that contain the new created key in the child level
			PageId childPageId	 //parameter indicate the new created node in the child level
			){
		//Establish a array to sort all keys
		//Temp arrays for insert and split
		int sortedKey[INTARRAYNONLEAFSIZE + 1];	
		PageId sortedPageNo[INTARRAYNONLEAFSIZE + 2];

		//allocate new memory space for the new Nonleafnode
		Page* newNonLeafPage;
		bufMgr->allocPage(file, newSplitPageId, newNonLeafPage);
		NonLeafNodeInt *newNonLeaf = (NonLeafNodeInt *) newNonLeafPage;	

		//make a copy of the original arrays
		for(int i = 1; i< INTARRAYNONLEAFSIZE+1; i++){	//i: 1-INRARRAYNONLEASIZE
			sortedPageNo[i] = node->pageNoArray[i - 1];  
			sortedKey[i] = node->keyArray[i - 1];
		}
		sortedPageNo[INTARRAYNONLEAFSIZE + 1] = node->pageNoArray[INTARRAYNONLEAFSIZE ]; 
		//Init both nonleaves
		for(int i = 0; i < INTARRAYNONLEAFSIZE + 1; i++){
			node->pageNoArray[i] = 0;
			newNonLeaf->pageNoArray[i] = 0;
		}

		/*****SORTING ALGORITHM STARTS*****/
		//Left shift 
		for(int i = 0; i < targetPos; i++){
			sortedKey[i] = sortedKey[i + 1];
			sortedPageNo[i] = sortedPageNo[i + 1];
		}
		//IF targetPos reaches, insert 
		sortedPageNo[targetPos] = childPageId ;
		sortedKey[targetPos ] = childKey;

		//Redistribution
		int midVal = (INTARRAYNONLEAFSIZE + 1) / 2;
		for(int i = 0; i < INTARRAYNONLEAFSIZE ; i++){
			if(i < midVal){    
				node->keyArray[i] = sortedKey[i];
				node->pageNoArray[i] = sortedPageNo[i];
			}
			else if(i == midVal){ 
				node->pageNoArray[midVal] = sortedPageNo[midVal];
			}
			else if(i > midVal){
				newNonLeaf->keyArray[i - 1 - midVal] = sortedKey[i]; 
				newNonLeaf->pageNoArray[i - 1 - midVal] = sortedPageNo[i];
			}

		}
		//Insert the last value in the PageNoArray
		newNonLeaf->pageNoArray[midVal - 1] = sortedPageNo[INTARRAYNONLEAFSIZE + 1];

		//Set up the return key for the new nonLeaf
		newPushedUpKey = sortedKey[midVal];	//this key will be deleted from the leaf
		newNonLeaf->level = node->level;

		//Release the page
		try{
			bufMgr->unPinPage(file,newSplitPageId, true);
		}
		catch (PageNotPinnedException e) {}	

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
		try{
			bufMgr->unPinPage(file,currentPageNum,false);
		}catch (PageNotPinnedException e) {}
		nextEntry = 0;	
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::findParentOfLeaf
	// Recursive call to find the parent node that contains the leaf which has the
	// key greater than lowVal in its keyArray
	// @param: lowVal: the lowValInt to be searched
	// @param  currPage: the current Page to be handle
	// @return: the child node that contains or its children contain the lowVal 
	// -----------------------------------------------------------------------------
	NonLeafNodeInt* BTreeIndex::findParentOfLeaf(int lowVal,PageId currPage){

		//Read info of the currentPae
		currentPageNum = currPage;
		bufMgr->readPage(file, currentPageNum, currentPageData);
		NonLeafNodeInt* currNode = (NonLeafNodeInt*) currentPageData;

		//Handle the data
		if(currNode->level == 1)
		{

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

		NonLeafNodeInt* retNode = findParentOfLeaf(lowVal,childPage); 
		return retNode;	
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

			//Increment the pointer
			nextEntry++;
			return;
		}
	}

	bool BTreeIndex::checkJumpPage(){
		LeafNodeInt* currNode = (LeafNodeInt *)currentPageData; 	

		//END OF ARRAY, jump to right sibling
		if(currNode->ridArray[nextEntry].page_number == 0||nextEntry == INTARRAYLEAFSIZE ){
			PageId nextNum = currNode->rightSibPageNo; 	
			try{
				bufMgr->unPinPage(file, currentPageNum, false);
			}catch (PageNotPinnedException e){}

			//if exhauested all possible value
			if(currNode->rightSibPageNo == 0)
				throw IndexScanCompletedException();

			//set the rightSibPageNo to next node to move to next node
			currentPageNum = nextNum;

			bufMgr->readPage(file,currentPageNum,currentPageData);
			//			try{
			//				bufMgr->unPinPage(file, currentPageNum, false);
			//			}catch (PageNotPinnedException e){ }
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
		catch(HashNotFoundException e){

		}
	}
}
