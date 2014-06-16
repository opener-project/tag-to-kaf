#!/usr/bin/env python

###############
## Changes ##
# 11-Mar-2014: included code to ignore D-neutral and generate triple on log output of check
#
########################

import sys
from collections import defaultdict
import codecs
import os
import shutil
from KafNafParserPy import *
from operator import itemgetter
import glob

NON_OPINIONATED='NON-OPINIONATED'

class My_annot:
    def __init__(self):
        self.wid = ''
        self.token = ''
        self.lemma = ''
        self.pos = ''
        self.opi_ent1 = ''
        self.opi_ent1_id = ''
        self.opinion1 = ''
        self.opinion1_id = ''
        self.opi_ent2 = ''
        self.opi_ent2_id = ''
        self.opinion2 = ''
        self.opinion2_id = ''
        self.opinionated = False
  
class My_annotations:
  def __init__(self,tag_filename):
    self.annots = []
    self.overall_rating = None
    if os.path.exists(tag_filename):
      fic = codecs.open(tag_filename,'r','utf-8',errors='ignore')
      for line in fic:
          fields = line.split('\t')
#          for n,f in enumerate(fields):
#              print n,f
#          print 
          anot = My_annot()
          anot.wid = fields[0]
          anot.token = fields[1]
          anot.lemma = fields[2]
          anot.pos = fields[3]
          #fields[4] is nothing
          anot.opi_ent1 = fields[5]
          anot.opi_ent1_id = fields[6]
          anot.opinion1 = fields[7]
          anot.opinion1_id = fields[8]
          
          anot.opi_ent2 = fields[9]
          anot.opi_ent2_id = fields[10]
          anot.opinion2 = fields[11]
          anot.opinion2_id = fields[12]
          
          if fields[13] == NON_OPINIONATED:
              anot.opinionated = True
          else:
              anot.opinionated = False
          
                    
          self.annots.append(anot)
      if self.overall_rating == '':
          self.overall_rating = None 
          
  def __iter__(self):
      for anot in self.annots:
          yield anot

          
  def _ss_repr__(self):
      r= ''
      s=set()
      for val in self.annots:
          s.add(val.token)
      return '\n'.join(list(s)).encode('utf-8')
              
      
def check_annotations(tag_filename, kaf_filename, log_notag2, log_triples,out,  log_potential):
    #print 'Processing',tag_filename
    basefile = os.path.basename(tag_filename)[:-4]
    annotations = My_annotations(tag_filename)
    opinions = {}
    count_types = {}
    tokens_notag2 = []
    str_for_sentence = {}
    sent_for_tokenid = {}
    
    kaf_obj = KafNafParser(kaf_filename)
    for token_obj in kaf_obj.get_tokens():
        token_str = token_obj.get_text()
        token_id = token_obj.get_id()
        token_sent = token_obj.get_sent()
        
        if token_sent not in str_for_sentence:
            str_for_sentence[token_sent]=''
        str_for_sentence[token_sent] += token_str+' '
    
        sent_for_tokenid[token_id] = token_sent

    first_opinion_per_sent = {}
    label_for_opinion_id = {}
    for anot in annotations:
        wid = anot.wid
        this_sent = sent_for_tokenid[wid]
        
        
        
        ##FOR THE FIRST OPINION        
        if anot.opi_ent1_id != '0' and anot.opinion1_id == '0':
            tokens_notag2.append(wid)
            
        if anot.opinion1_id is not None and anot.opinion1_id !='0':
            if not anot.opinion1_id in opinions: opinions[anot.opinion1_id] = []
            opinions[anot.opinion1_id].append((anot.opi_ent1,wid,anot.token,anot.opi_ent1_id))
            label_for_opinion_id[anot.opinion1_id] = (anot.opinion1,this_sent)
            
            if this_sent not in first_opinion_per_sent:
                first_opinion_per_sent[this_sent] = anot.opinion1_id
            
        ##########
        
        ##FOR THE SECOND OPINION        

        if anot.opi_ent2_id != '0' and anot.opinion2_id == '0':
            tokens_notag2.append(wid)
            
        if anot.opinion2_id is not None and anot.opinion2_id !='0':
            if not anot.opinion2_id in opinions: opinions[anot.opinion2_id] = []
            opinions[anot.opinion2_id].append((anot.opi_ent2,wid,anot.token,anot.opi_ent2_id))
            label_for_opinion_id[anot.opinion2_id] = (anot.opinion2,this_sent)
            
            if this_sent not in first_opinion_per_sent:
                first_opinion_per_sent[this_sent] = anot.opinion2_id
                
                    
            


    ######
    ## Deal with the cases OH1-OP
    for opi_id, (label,sent) in label_for_opinion_id.items():
        if label == 'OH1-OP':
            opi_id_first_in_sent = first_opinion_per_sent[sent] # The identifier of the first opinion in the sentence
            first_opi = opinions[opi_id_first_in_sent]  #The span of opi entities for the first opinion
            
            prv=None
            for ent, wid, token, ent_id in first_opi:
                if ent == 'OpinionHolder' and (prv is None or ent_id==prv):
                    prv=ent_id
                    opinions[opi_id].append((ent, wid, token, ent_id))

        
    ##Resolve opinions
    for opinion_id, tokens in opinions.items():
        elements = {}
        num_empty = 0
        this_sent = None
        
        previous_type=None
        ents_already_tagged = set()
        crossing_ents = False
        whole_opinion_str = basefile+' Opinion-id:'+opinion_id+'\n'
        for ent, wid, token, ent_id in tokens:
            my_ent = ent
            if my_ent=='': my_ent='none     '
            whole_opinion_str += '\t'+my_ent+'\t'+ent_id+'\t'+token+'\n'
            ##The tokens are in order
            which_type = None
            if ent == 'OpinionTarget': 
                which_type='target'
            elif ent == 'OpinionHolder': 
                which_type='holder'
            elif ent == 'D-Neutral':
                # The D-neutrals are not considered for checking the crossing criterium
                # and will not be later considered to be included as opinion triple
                which_type = None    
            elif ent!='': 
                which_type='expression'
            
            ##This will crossing annotations
            if which_type is not None:
                if previous_type == None or which_type != previous_type:
                    if which_type in ents_already_tagged:
                        crossing_ents = True
                    ents_already_tagged.add(which_type)   
                previous_type = which_type
            
            if this_sent is None:
                this_sent = sent_for_tokenid[wid]
                
            if ent_id == '0': 
                num_empty += 1
            if not (ent,ent_id) in elements:
                elements[(ent,ent_id)] = [(wid,token)]
            else:
                elements[(ent,ent_id)].append((wid,token))
                
                

            
                        
        num_exp = num_tar = num_hol = 0
        targets = []
        holders = []
        expressions = []
        for a, b in elements.items():
            if a[0] == 'OpinionTarget': 
                num_tar+=1
                targets.append(b)
            elif a[0] == 'OpinionHolder': 
                num_hol+=1
                holders.append(b)
            elif a[0]!='' and a[0] != 'D-Neutral': 
                num_exp+=1 
                expressions.append((a[0],b))

        error_triples = False
        if num_exp >=2 and (num_hol>=2 or num_tar>=2):
            error_triples = True
        elif num_hol >= 2 and (num_exp>= 2 or num_tar>=2):
            error_triples = True
        elif num_tar >= 2 and (num_hol>=2 or num_exp>=2):
            error_triples = True
            
        ##Printing to the out file
        print>>out,'Opinion id',opinion_id,' ',str_for_sentence[this_sent].encode('utf-8')
        print>>out,'\tCrossing error:',crossing_ents
        print>>out,'\tMore than 2 holders/targets/expressions:',error_triples
        print>>out,'\tWill be discarded:',(crossing_ents and error_triples)
        print>>out
        print>>out,'\tAnnotated opinion entities'
        for (ent, ent_id), eles in elements.items():
            if ent_id=='0': ent='EMPTY'
            print>>out,'\t  ',ent,' id:'+ent_id,'==>', eles
           
        if len(targets) == 0:
            targets = [[]]
        if len(holders) == 0:
            holders = [[]]
        nt = 0
        print>>out
        print>>out,'\tTriples created:'
        for ent_type, list_wid_token in expressions:
            str_exp = ' '.join(token for wid,token in list_wid_token)
            for list_wid_token_tar in targets:
                str_tar = ' '.join(token for wid,token in list_wid_token_tar)
                for list_wif_token_hol in holders:
                    str_hol = ' '.join(token for wid,token in list_wif_token_hol)
                    print>>out,'\t  Triple',nt
                    print>>out, '\t    Expression:',str_exp.encode('utf-8')
                    print>>out, '\t    Target:',str_tar.encode('utf-8')
                    print>>out, '\t    Holder:',str_hol.encode('utf-8')
                    nt+=1
        print>>out
            
        if crossing_ents and error_triples:
            print>>log_potential,whole_opinion_str.encode('utf-8')
            
            
        if error_triples:
            print>>log_triples,basefile,'Opinion-id:',opinion_id
            print>>log_triples,'\tNum expressions: ',num_exp
            print>>log_triples,'\tNum targets:',num_tar
            print>>log_triples,'\tNum holders:',num_hol
     
    
    if len(tokens_notag2) != 0:
        print>>log_notag2,basefile
        print>>log_notag2,'\t',' '.join(tokens_notag2)
        
        


    
def check_list_files(tag_folder,kaf_folder, analysis_folder):
    if os.path.exists(analysis_folder):
        shutil.rmtree(analysis_folder)
    os.mkdir(analysis_folder)
    os.mkdir(analysis_folder+'/annotated_opinions')
    
    log_notag2 = open(analysis_folder+'/log_notags_level2.txt','w')
    log_triples = open(analysis_folder+'/log_multiple_triples.txt','w')
    log_potential = open(analysis_folder+'/log_crossed_annotations.txt','w')
    
    num_kaf = 0
    num_tag = 0
    #for tag_file in glob.glob(tag_folder+'/*.tag'):  
    for kaf_file in glob.glob(kaf_folder+'/*.kaf'):
        num_kaf += 1
        basefile = os.path.basename(kaf_file)[:-4]
        tag_file = tag_folder+'/'+basefile+'.tag'
        if os.path.exists(tag_file):
            num_tag += 1
            out_file = analysis_folder+'/annotated_opinions/'+basefile
            out = open(out_file,'w')       
            check_annotations(tag_file ,kaf_file, log_notag2,  log_triples,out,log_potential)
            out.close()
            
    print 'Processed ', tag_folder
    print '\tNum KAF files:',num_kaf
    print '\tNum TAG files:',num_tag
    log_triples.close()
    log_notag2.close()
    log_potential.close()
   

def map_tokens_to_terms(list_tokens,mapping):
    return [mapping.get(token_id) for token_id in list_tokens]


def create_mapping_token_to_term(knaf_obj):
    term_for_token = {}
    for term in knaf_obj.get_terms():
        term_id = term.get_id()
        span_tokens = term.get_span().get_span_ids()
        
        for token_id in span_tokens:
            term_for_token[token_id] = term_id
            
    return term_for_token

    
    
def extract_opinions_from_file(kaf_filename,tag_filename,out_filename):
    #print 'Processing',tag_filename
    basefile = os.path.basename(tag_filename)[:-4]
    annotations = My_annotations(tag_filename)
    opinions = {}
    count_types = {}
    tokens_notag2 = []
    str_for_sentence = {}
    sent_for_tokenid = {}
    num_triples = 0 
    num_scope_opis = 0
    num_skipped = 0
    kaf_obj = KafNafParser(kaf_filename)
    kaf_obj.remove_opinion_layer()

    term_for_token = create_mapping_token_to_term(kaf_obj)
    all_sent_ids = set()
    sent_ids_with_opinion = set()
    
    
    for token_obj in kaf_obj.get_tokens():
        token_str = token_obj.get_text()
        token_id = token_obj.get_id()
        token_sent = token_obj.get_sent()
        all_sent_ids.add(token_sent)
        
        if token_sent not in str_for_sentence:
            str_for_sentence[token_sent]=''
        str_for_sentence[token_sent] += token_str+' '
    
        sent_for_tokenid[token_id] = token_sent

    
    first_opinion_per_sent = {}
    label_for_opinion_id = {}
    non_opinionated_token_ids = []
    for anot in annotations:
        wid = anot.wid
        this_sent = sent_for_tokenid[wid]
        
        #For keeping track of the tokens marke as non opinionated
        if anot.opinionated:
            non_opinionated_token_ids.append(wid)
        
        
        ##FOR THE FIRST OPINION        
        if anot.opi_ent1_id != '0' and anot.opinion1_id == '0':
            tokens_notag2.append(wid)
            
        if anot.opinion1_id is not None and anot.opinion1_id !='0':
            if not anot.opinion1_id in opinions: opinions[anot.opinion1_id] = []
            opinions[anot.opinion1_id].append((anot.opi_ent1,wid,anot.token,anot.opi_ent1_id))
            label_for_opinion_id[anot.opinion1_id] = (anot.opinion1,this_sent)
            
            if this_sent not in first_opinion_per_sent:
                first_opinion_per_sent[this_sent] = anot.opinion1_id
            
        ##########
        
        ##FOR THE SECOND OPINION        

        if anot.opi_ent2_id != '0' and anot.opinion2_id == '0':
            tokens_notag2.append(wid)
            
        if anot.opinion2_id is not None and anot.opinion2_id !='0':
            if not anot.opinion2_id in opinions: opinions[anot.opinion2_id] = []
            opinions[anot.opinion2_id].append((anot.opi_ent2,wid,anot.token,anot.opi_ent2_id))
            label_for_opinion_id[anot.opinion2_id] = (anot.opinion2,this_sent)
            
            if this_sent not in first_opinion_per_sent:
                first_opinion_per_sent[this_sent] = anot.opinion2_id
                

    ######
    ## Deal with the cases OH1-OP
    for opi_id, (label,sent) in label_for_opinion_id.items():
        if label == 'OH1-OP':
            opi_id_first_in_sent = first_opinion_per_sent[sent] # The identifier of the first opinion in the sentence
            first_opi = opinions[opi_id_first_in_sent][:]  #The span of opi entities for the first opinion
            prv=None
            for ent, wid, token, ent_id in first_opi:
                if ent == 'OpinionHolder' and (prv is None or ent_id==prv):
                    prv=ent_id
                    opinions[opi_id].append((ent, wid, token, ent_id))

    ##Resolve opinions
    for opinion_id, tokens in opinions.items():
        num_scope_opis += 1
        str_opi = '\t'+tag_filename+' opinion id:'+opinion_id+'\n'
        elements = {}
        num_empty = 0
        this_sent = None
        
        previous_type=None
        ents_already_tagged = set()
        crossing_ents = False
        whole_opinion_str = basefile+' Opinion-id:'+opinion_id+'\n'
        for ent, wid, token, ent_id in tokens:
            my_ent = ent
            if my_ent=='': my_ent='none     '
            whole_opinion_str += '\t'+my_ent+'\t'+ent_id+'\t'+token+'\n'
            ##The tokens are in order
            which_type = None
            if ent == 'OpinionTarget': 
                which_type='target'
            elif ent == 'OpinionHolder': 
                which_type='holder'
            elif ent == 'D-Neutral':
                # The D-neutrals are not considered for checking the crossing criterium
                # and will not be later considered to be included as opinion triple
                which_type = None    
            elif ent!='': 
                which_type='expression'
            
            ##This will crossing annotations
            if which_type is not None:
                if previous_type == None or which_type != previous_type:
                    if which_type in ents_already_tagged:
                        crossing_ents = True
                    ents_already_tagged.add(which_type)   
                previous_type = which_type
            
            if this_sent is None:
                this_sent = sent_for_tokenid[wid]
                
            if ent_id == '0': 
                num_empty += 1
            if not (ent,ent_id) in elements:
                elements[(ent,ent_id)] = [(wid,token)]
            else:
                elements[(ent,ent_id)].append((wid,token))
                
                        
        num_exp = num_tar = num_hol = 0
        targets = []
        holders = []
        expressions = []
        for a, b in elements.items():
            if a[0] == 'OpinionTarget': 
                num_tar+=1
                targets.append(b)
            elif a[0] == 'OpinionHolder': 
                num_hol+=1
                holders.append(b)
            elif a[0]!='' and a[0] != 'D-Neutral': 
                num_exp+=1 
                expressions.append((a[0],b))

        error_triples = False
        if num_exp >=2 and (num_hol>=2 or num_tar>=2):
            error_triples = True
        elif num_hol >= 2 and (num_exp>= 2 or num_tar>=2):
            error_triples = True
        elif num_tar >= 2 and (num_hol>=2 or num_exp>=2):
            error_triples = True
            
        triples = []
        if len(targets) == 0:
            targets = [[]]
        if len(holders) == 0:
            holders = [[]]
            

        if not error_triples and crossing_ents:
            print '\tDiscarded ',str_opi.encode('utf-8')
            num_skipped += 1
        else:
            for ent_type, list_wid_token in expressions:
                span_exp = map_tokens_to_terms([wid for wid,token in list_wid_token],term_for_token)
                str_exp = ' '.join(token for wid,token in list_wid_token)
                
                #Add the sentence to opinionated sentence
                for wid,token in list_wid_token:
                    sent_ids_with_opinion.add(sent_for_tokenid[wid])
                
                for list_wid_token_tar in targets:
                    span_tar = map_tokens_to_terms([wid for wid,token in list_wid_token_tar],term_for_token)
                    str_tar = ' '.join(token for wid,token in list_wid_token_tar)
                    
                    for list_wif_token_hol in holders:
                        span_hol = map_tokens_to_terms([wid for wid,token in list_wif_token_hol],term_for_token)
                        str_hol = ' '.join(token for wid,token in list_wif_token_hol)
                        
                        triples.append((ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol))
        
        ##Convert triples to opinions
        for n, (ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol) in enumerate(triples):
            
            holder = Cholder()
            if len(span_hol) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_hol)
                holder.set_span(span_obj)
                holder.set_comment(str_hol)
                
            target = opinion_data.Ctarget()
            if len(span_tar) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_tar)
                target.set_span(span_obj)
                target.set_comment(str_tar)
                
            exp = Cexpression()
            span_obj = Cspan()
            span_obj.create_from_ids(span_exp)
            exp.set_span(span_obj)
            exp.set_polarity(ent_type)
            exp.set_comment(str_exp)
            
            my_opinion = Copinion(type='KAF')
            my_opinion.set_holder(holder)
            my_opinion.set_target(target)
            my_opinion.set_expression(exp)
            my_opinion.set_comment('Tag opinion scope id='+str(opinion_id))
            my_opinion.set_id('o'+str(num_triples))
            kaf_obj.add_opinion(my_opinion)
            num_triples += 1
            
    
    ##Create a 'fake' opinion with all the nono opinionated toks as the expression
    if len(non_opinionated_token_ids) != 0:
        span_exp = map_tokens_to_terms(non_opinionated_token_ids,term_for_token)
        exp = Cexpression()
        span_obj = Cspan()
        span_obj.create_from_ids(span_exp)
        exp.set_span(span_obj)
        exp.set_polarity(NON_OPINIONATED)
        my_opinion = Copinion(type='KAF')
        my_opinion.set_expression(exp)
        my_opinion.set_comment('Marked as NON-OPINIONATED in the tag files')
        my_opinion.set_id('o'+str(num_triples))
        kaf_obj.add_opinion(my_opinion)


    ##Add the linguistic processor
    kaf_obj.add_linguistic_processor('opinions',Clp(name='Manual annotations', version='1.0'))
    kaf_obj.dump(out_filename)

    num_sentences = len(all_sent_ids)
    num_sentences_with_opinions = len(sent_ids_with_opinion)
    
    return num_scope_opis, num_skipped, num_triples, num_sentences, num_sentences_with_opinions
            

            
def extract_opinions_from_file_old(kaf_filename, tag_filename,out_filename):
    knaf_obj = KafNafParser(kaf_filename)

    annotations = My_annotations(tag_filename)

    term_for_token = create_mapping_token_to_term(knaf_obj)
    
          
            
    
    opinions = {}
    aspects = {}
    num_triples = 0
    num_scope_opis = 0
    num_skipped = 0 
    for anot in annotations:
        wid = anot.wid
        
        opi_ent = anot.opinion_entity
        op_id = anot.opinion_id
        if op_id is not None and op_id != '0':
            if not op_id in opinions:
                opinions[op_id] = []
            opi_ent = anot.opinion_entity
            opinions[op_id].append((anot.opinion_entity,wid,anot.token, anot.opinion_entity_id))
 
        aspect_label = anot.aspect
        aspect_id = anot.aspect_id       
        if aspect_id is not None and aspect_id != '0':
            if aspect_id not in aspects:
                aspects[aspect_id] = []
            aspects[aspect_id].append((aspect_label,wid,anot.token))
    
    
        ######
    ## Deal with the cases OH1-OP
    for opi_id, (label,sent) in label_for_opinion_id.items():
        if label == 'OH1-OP':
            opi_id_first_in_sent = first_opinion_per_sent[sent] # The identifier of the first opinion in the sentence
            first_opi = opinions[opi_id_first_in_sent]  #The span of opi entities for the first opinion
            
            prv=None
            for ent, wid, token, ent_id in first_opi:
                if ent == 'OpinionHolder' and (prv is None or ent_id==prv):
                    prv=ent_id
                    opinions[opi_id].append((ent, wid, token, ent_id))
    
   
    str_opi = tag_filename+' opinion id'
    for opi_id, list_tokens in opinions.items():
        num_scope_opis += 1
        str_opi = '\t'+tag_filename+' opinion id:'+opi_id+'\n'
        elements = {}
        previous_type=None
        ents_already_tagged = set()
        crossing_ents = False
        for ent, wid, token, ent_id in list_tokens:
            
            
            which_type = None
            if ent == 'OpinionTarget': 
                which_type='target'
            elif ent == 'OpinionHolder': 
                which_type='holder'
            elif ent!='': 
                which_type='expression'
            myent= ent
            if ent=='': myent='none    '
            str_opi+='\t\t'+myent+'\t'+token+'\n'
            
            ##This will crossing annotations
            if which_type is not None:
                if previous_type == None or which_type != previous_type:
                    if which_type in ents_already_tagged:
                        crossing_ents = True
                    ents_already_tagged.add(which_type)   
                previous_type = which_type
                
                
            if not (ent,ent_id) in elements:
                elements[(ent,ent_id)] = [(wid,token)]
                #elements[(ent,ent_id)] = [wid]
            else:
                elements[(ent,ent_id)].append((wid,token))
                #elements[(ent,ent_id)].append(wid)

                        
        #print 'Opinion',opi_id,tag_filename
        num_exp = num_tar = num_hol = 0
        targets = []
        holders = []
        expressions = []
        for a, b in elements.items():
            if a[0] == 'OpinionTarget': 
                num_tar+=1
                targets.append(b)
            elif a[0] == 'OpinionHolder': 
                num_hol+=1
                holders.append(b)
            elif a[0]!='': 
                num_exp+=1 
                expressions.append((a[0],b))
        
        opinion_is_ok = True
        if num_exp >=2 and (num_hol>=2 or num_tar>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 expresssions and 2 targets/holders'
        elif num_hol >= 2 and (num_exp>= 2 or num_tar>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 holders and 2 exp/targets'
        elif num_tar >= 2 and (num_hol>=2 or num_exp>=2):
            opinion_is_ok = False
            #print '\t\tERROR 2 targets and 2 exp/holders'
        
        
        triples = []
        if len(targets) == 0:
            targets = [[]]
        if len(holders) == 0:
            holders = [[]]
            

        if not opinion_is_ok and crossing_ents:
            print '\tDiscarded ',str_opi.encode('utf-8')
            num_skipped += 1
        else:
            for ent_type, list_wid_token in expressions:
                span_exp = map_tokens_to_terms([wid for wid,token in list_wid_token],term_for_token)
                str_exp = ' '.join(token for wid,token in list_wid_token)
                
                for list_wid_token_tar in targets:
                    span_tar = map_tokens_to_terms([wid for wid,token in list_wid_token_tar],term_for_token)
                    str_tar = ' '.join(token for wid,token in list_wid_token_tar)
                    
                    for list_wif_token_hol in holders:
                        span_hol = map_tokens_to_terms([wid for wid,token in list_wif_token_hol],term_for_token)
                        str_hol = ' '.join(token for wid,token in list_wif_token_hol)
                        
                        triples.append((ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol))
        
        ##Convert triples to opinions
        for n, (ent_type,span_exp,str_exp,span_tar,str_tar,span_hol,str_hol) in enumerate(triples):
            
            holder = Cholder()
            if len(span_hol) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_hol)
                holder.set_span(span_obj)
                holder.set_comment(str_hol)
                
            target = opinion_data.Ctarget()
            if len(span_tar) != 0:
                span_obj = Cspan()
                span_obj.create_from_ids(span_tar)
                target.set_span(span_obj)
                target.set_comment(str_tar)
                
            exp = Cexpression()
            span_obj = Cspan()
            span_obj.create_from_ids(span_exp)
            exp.set_span(span_obj)
            exp.set_polarity(ent_type)
            exp.set_comment(str_exp)
            
            my_opinion = Copinion(type='KAF')
            my_opinion.set_holder(holder)
            my_opinion.set_target(target)
            my_opinion.set_expression(exp)
            my_opinion.set_id(opi_id+'_'+str(n))
            knaf_obj.add_opinion(my_opinion)
            num_triples += 1
            
    ##Add the linguistic processor
    knaf_obj.add_linguistic_processor('opinions',Clp(name='Manual annotations', version='1.0'))
    knaf_obj.dump(out_filename)
    return num_scope_opis, num_skipped, num_triples
                  
        
        
    
def extract_opinions(tag_folder,kaf_folder,out_folder):
    
    if not os.path.exists(out_folder):
        os.mkdir(out_folder)
        
    #For every kaf file in the kaf_folder
    num_kaf_files = num_tag_files = total_scope_opis = total_skipped = total_triples = 0
    total_sents = total_sents_opi = 0 
    
    for kaf_filename in glob.glob(kaf_folder+'/*.kaf'):
        num_kaf_files += 1
        base_filename = os.path.basename(kaf_filename)[:-4]
        out_filename = out_folder+'/'+base_filename+'.kaf'
        print 'Processing ',base_filename
        tag_filename = tag_folder+'/'+base_filename+'.tag'
        
        if not os.path.exists(tag_filename):
            print '\t TAG file not found on ',tag_filename
        else:
            num_tag_files += 1
            num_scope_opis, num_skipped, num_triples, num_sentences, num_sents_opis = extract_opinions_from_file(kaf_filename,tag_filename,out_filename)
            total_scope_opis += num_scope_opis
            total_skipped += num_skipped
            total_triples += num_triples
            total_sents += num_sentences
            total_sents_opi += num_sents_opis
            print '\tTotal of opinions with different scope in the tag file:',num_scope_opis
            print '\tTotal of skipped opinions:',num_skipped
            print '\tTotal of opinion triples generated:',num_triples
            print '\tTotal of sentences in the tag file',num_sentences
            print '\tTotal of sentences with annotated opinions',num_sents_opis
    print 
    print '#'*25
    print 'Total KAF files ', num_kaf_files
    print 'Total TAG files ', num_tag_files
    print 'Total sentences in all TAG files', total_sents
    print 'Total sentences in all TAG files with annotated opinions' , total_sents_opi
    print '% of sentences with opinions: ', round((total_sents_opi*100.0/total_sents),2)
    print 'Opinion density (avg number of opinions per sentence)'
    print '  Considering ALL sentences: %.2f' % (total_scope_opis*1.0/total_sents)
    print '  Considering ANNOTATED sentences: %.2f' % (total_scope_opis*1.0/total_sents_opi)
    print 'Total opinions-scope in tag',total_scope_opis
    print 'Total skipped opinions',total_skipped
    print 'Total generated triples',total_triples
    print '#'*25
    print
    

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print 'Usage: ',sys.argv[0],' -check tag_folder kaf_folder out_folder --> for checking'
        print sys.argv[0],' -generate tag_folder kaf_folder out_folder --> for generating opinions'
        sys.exit(-1)
    if sys.argv[1] == '-check':
        #tag_folder = '/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_tag'
        tag_folder = sys.argv[2]
        kaf_folder = sys.argv[3]
        out_folder = sys.argv[4]
        check_list_files(tag_folder,kaf_folder,out_folder)
    elif sys.argv[1] == '-generate':
        tag_folder = sys.argv[2] ##'/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_tag'
        kaf_folder = sys.argv[3] ##'/home/izquierdo/data/opener_annotation/hotel_reviews_set1/dutch_hotel_set1_treetagger_kaf'
        out_folder = sys.argv[4]
        extract_opinions(tag_folder,kaf_folder,out_folder)
    elif sys.argv[1] == '-extract_aspects':
        tag_folder = sys.argv[2]
    else:
        print 'Unknow option',sys.argv[1]
    sys.exit(0)
    

    
